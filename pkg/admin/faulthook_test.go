package admin

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// faultHook is a go-redis Hook that injects a configured error for commands whose
// name matches cmdName. It lets tests exercise mid-operation Redis failures (e.g. a
// GET returning redis.Nil during a SCAN walk, or a pipelined DEL failing) that are
// otherwise hard to reproduce deterministically against an in-memory server.
type faultHook struct {
	// cmdName is the lowercase command name to fault (e.g. "get", "del").
	cmdName string
	// err is returned in place of executing the matched command.
	err error
}

func (h faultHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h faultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == h.cmdName {
			cmd.SetErr(h.err)
			return h.err
		}

		return next(ctx, cmd)
	}
}

func (h faultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == h.cmdName {
				cmd.SetErr(h.err)
				return h.err
			}
		}

		return next(ctx, cmds)
	}
}
