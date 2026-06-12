package scheduler

import (
	"context"
	"errors"
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

// LeaderElector manages distributed leader election using Redis
type LeaderElector interface {
	Start(ctx context.Context) error
	Stop() error
	IsLeader() bool

	// PromotedChan returns a channel that signals when this instance is promoted to leader
	PromotedChan() <-chan struct{}
	// DemotedChan returns a channel that signals when this instance is demoted from leader
	DemotedChan() <-chan struct{}
}

// elector implements the LeaderElector interface
type elector struct {
	log        logrus.FieldLogger
	redis      *redis.Client
	instanceID string
	leaderKey  string

	isLeader bool
	mu       sync.RWMutex

	// renewInterval and leaseTTL are fields (defaulting to the package-level
	// constants) so tests can drive the election loop with short intervals.
	renewInterval time.Duration
	leaseTTL      time.Duration

	done chan struct{}
	wg   sync.WaitGroup

	promoted chan struct{}
	demoted  chan struct{}
}

// NewLeaderElector creates a new leader elector instance
func NewLeaderElector(log logrus.FieldLogger, redisOpt *redis.Options) LeaderElector {
	instanceID := uuid.New().String()

	return &elector{
		log:           log.WithField("component", "election"),
		redis:         redis.NewClient(redisOpt),
		instanceID:    instanceID,
		leaderKey:     leaderKeyPrefix,
		renewInterval: renewInterval,
		leaseTTL:      leaseTTL,
		done:          make(chan struct{}),
		promoted:      make(chan struct{}, 1),
		demoted:       make(chan struct{}, 1),
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

	ticker := time.NewTicker(e.renewInterval)
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
	err := e.redis.SetArgs(ctx, e.leaderKey, e.instanceID, redis.SetArgs{
		Mode: "NX",
		TTL:  e.leaseTTL,
	}).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		e.log.WithError(err).Debug("Failed to acquire leader lock")
		return false
	}

	if err == nil {
		e.log.WithFields(logrus.Fields{
			"instance_id": e.instanceID,
			"ttl":         e.leaseTTL,
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
		if err := e.redis.Expire(ctx, e.leaderKey, e.leaseTTL).Err(); err != nil {
			e.log.WithError(err).Warn("Failed to renew leader lease")
			return false
		}

		e.log.WithFields(logrus.Fields{
			"instance_id": e.instanceID,
			"ttl":         e.leaseTTL,
		}).Debug("Renewed leader lease")
		return true
	}

	e.log.WithFields(logrus.Fields{
		"current_leader": owner,
		"instance_id":    e.instanceID,
	}).Debug("Another instance holds leadership")

	return false
}

// relinquishScript deletes the leader lock only if this instance still owns
// it. The check and delete must be atomic: a GET-then-DEL pair can delete a
// lock that another instance acquired after our lease expired.
const relinquishScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`

func (e *elector) relinquish(ctx context.Context) {
	if !e.IsLeader() {
		return
	}

	deleted, err := redis.NewScript(relinquishScript).Run(ctx, e.redis, []string{e.leaderKey}, e.instanceID).Int()
	if err != nil {
		e.log.WithError(err).Warn("Failed to delete leader lock")
	} else if deleted == 1 {
		e.log.WithField("instance_id", e.instanceID).Info("Relinquished leader lock")
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

func (e *elector) PromotedChan() <-chan struct{} {
	return e.promoted
}

func (e *elector) DemotedChan() <-chan struct{} {
	return e.demoted
}

var _ LeaderElector = (*elector)(nil)
