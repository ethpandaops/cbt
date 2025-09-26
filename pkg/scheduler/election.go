package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	leaderKeyPrefix = "cbt:scheduler:leader"
	leaseTTL        = 10 * time.Second
	renewInterval   = 3 * time.Second
)

var (
	// ErrElectorStopped is returned when the elector is stopped while waiting for leadership
	ErrElectorStopped = errors.New("elector stopped while waiting for leadership")
)

// LeaderElector manages distributed leader election using Redis
type LeaderElector interface {
	Start(ctx context.Context) error
	Stop() error
	IsLeader() bool
	WaitForLeadership(ctx context.Context) error
}

// elector implements the LeaderElector interface
type elector struct {
	log        logrus.FieldLogger
	redis      *redis.Client
	instanceID string
	leaderKey  string

	isLeader bool
	mu       sync.RWMutex

	done chan struct{}
	wg   sync.WaitGroup

	promoted chan struct{}
	demoted  chan struct{}
}

// NewLeaderElector creates a new leader elector instance
func NewLeaderElector(log logrus.FieldLogger, redisOpt *redis.Options) LeaderElector {
	instanceID := uuid.New().String()

	return &elector{
		log:        log.WithField("component", "election"),
		redis:      redis.NewClient(redisOpt),
		instanceID: instanceID,
		leaderKey:  leaderKeyPrefix,
		done:       make(chan struct{}),
		promoted:   make(chan struct{}, 1),
		demoted:    make(chan struct{}, 1),
	}
}

func (e *elector) Start(ctx context.Context) error {
	e.log.WithField("instance_id", e.instanceID).Info("Starting leader election")

	e.wg.Add(1)
	go e.run(ctx)

	return nil
}

func (e *elector) Stop() error {
	e.log.Info("Stopping leader election")
	close(e.done)

	e.relinquish(context.Background())

	e.wg.Wait()

	if err := e.redis.Close(); err != nil {
		e.log.WithError(err).Warn("Failed to close Redis client")
	}

	e.log.Info("Leader election stopped")
	return nil
}

func (e *elector) run(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.done:
			return

		case <-ctx.Done():
			return

		case <-ticker.C:
			wasLeader := e.IsLeader()
			acquired := e.tryAcquire(ctx)

			if acquired && !wasLeader {
				e.setLeader(true)
				e.log.WithField("instance_id", e.instanceID).Info("Promoted to leader")

				select {
				case e.promoted <- struct{}{}:
				default:
				}
			} else if !acquired && wasLeader {
				e.setLeader(false)
				e.log.WithField("instance_id", e.instanceID).Info("Demoted from leader")

				select {
				case e.demoted <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (e *elector) tryAcquire(ctx context.Context) bool {
	result, err := e.redis.SetNX(ctx, e.leaderKey, e.instanceID, leaseTTL).Result()
	if err != nil {
		e.log.WithError(err).Debug("Failed to acquire leader lock")
		return false
	}

	if result {
		e.log.WithFields(logrus.Fields{
			"instance_id": e.instanceID,
			"ttl":         leaseTTL,
		}).Debug("Acquired leader lock")
		return true
	}

	owner, err := e.redis.Get(ctx, e.leaderKey).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			e.log.WithError(err).Debug("Failed to check lock owner")
		}
		return false
	}

	if owner == e.instanceID {
		if err := e.redis.Expire(ctx, e.leaderKey, leaseTTL).Err(); err != nil {
			e.log.WithError(err).Warn("Failed to renew leader lease")
			return false
		}

		e.log.WithFields(logrus.Fields{
			"instance_id": e.instanceID,
			"ttl":         leaseTTL,
		}).Debug("Renewed leader lease")
		return true
	}

	e.log.WithFields(logrus.Fields{
		"current_leader": owner,
		"instance_id":    e.instanceID,
	}).Debug("Another instance holds leadership")

	return false
}

func (e *elector) relinquish(ctx context.Context) {
	if !e.IsLeader() {
		return
	}

	owner, err := e.redis.Get(ctx, e.leaderKey).Result()
	if err == nil && owner == e.instanceID {
		if err := e.redis.Del(ctx, e.leaderKey).Err(); err != nil {
			e.log.WithError(err).Warn("Failed to delete leader lock")
		} else {
			e.log.WithField("instance_id", e.instanceID).Info("Relinquished leader lock")
		}
	}

	e.setLeader(false)
}

func (e *elector) setLeader(isLeader bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLeader = isLeader
}

func (e *elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *elector) WaitForLeadership(ctx context.Context) error {
	if e.IsLeader() {
		return nil
	}

	e.log.Info("Waiting for leadership promotion")

	select {
	case <-e.promoted:
		e.log.Info("Leadership acquired")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting for leadership: %w", ctx.Err())
	case <-e.done:
		return ErrElectorStopped
	}
}

func (e *elector) PromotedChan() <-chan struct{} {
	return e.promoted
}

func (e *elector) DemotedChan() <-chan struct{} {
	return e.demoted
}

var _ LeaderElector = (*elector)(nil)
