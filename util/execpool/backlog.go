
package execpool

import (
	"context"
	"sync"
)

type backlog struct {
	pool      ExecutionPool
	wg        sync.WaitGroup
	buffer    chan backlogItemTask
	ctx       context.Context
	ctxCancel context.CancelFunc
	owner     interface{}
	priority  Priority
}

type backlogItemTask struct {
	enqueuedTask
	priority Priority
}

type BacklogPool interface {
	ExecutionPool
	EnqueueBacklog(enqueueCtx context.Context, t ExecFunc, arg interface{}, out chan interface{}) error
}

func MakeBacklog(execPool ExecutionPool, backlogSize int, priority Priority, owner interface{}) BacklogPool {
	if backlogSize < 0 {
		return nil
	}
	bl := &backlog{
		pool:     execPool,
		owner:    owner,
		priority: priority,
	}
	bl.ctx, bl.ctxCancel = context.WithCancel(context.Background())
	if bl.pool == nil {
		bl.pool = MakePool(bl)
	}
	if backlogSize == 0 {
		backlogSize = bl.pool.GetParallelism()
	}
	bl.buffer = make(chan backlogItemTask, backlogSize)

	bl.wg.Add(1)
	go bl.worker()
	return bl
}

func (b *backlog) GetParallelism() int {
	return b.pool.GetParallelism()
}

func (b *backlog) Enqueue(enqueueCtx context.Context, t ExecFunc, arg interface{}, priority Priority, out chan interface{}) error {
	select {
	case b.buffer <- backlogItemTask{
		enqueuedTask: enqueuedTask{
			execFunc: t,
			arg:      arg,
			out:      out,
		},
		priority: priority,
	}:
		return nil
	case <-enqueueCtx.Done():
		return enqueueCtx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

func (b *backlog) EnqueueBacklog(enqueueCtx context.Context, t ExecFunc, arg interface{}, out chan interface{}) error {
	select {
	case b.buffer <- backlogItemTask{
		enqueuedTask: enqueuedTask{
			execFunc: t,
			arg:      arg,
			out:      out,
		},
		priority: b.priority,
	}:
		return nil
	case <-enqueueCtx.Done():
		return enqueueCtx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	}
}

func (b *backlog) Shutdown() {
	b.ctxCancel()
	b.wg.Wait()
	if b.pool.GetOwner() == b {
		b.pool.Shutdown()
	}
}

func (b *backlog) worker() {
	var t backlogItemTask
	var ok bool
	defer b.wg.Done()

	for {

		select {
		case t, ok = <-b.buffer:
		case <-b.ctx.Done():
			return
		}

		if !ok {
			return
		}

		if b.pool.Enqueue(b.ctx, t.execFunc, t.arg, t.priority, t.out) != nil {
			break
		}
	}
}

func (b *backlog) GetOwner() interface{} {
	return b.owner
}
