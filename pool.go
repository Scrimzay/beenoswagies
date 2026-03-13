package beenoswagies

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type Pool[T any] struct {
	workerChan chan holder[T]
	workerWait sync.WaitGroup
	BufferSize int
	stats stats
}

type stats struct {
	Running atomic.Int64
}

type StatsSnapshot struct {
	Queued int64
	Running int64
}

type Job[T any] func() (T, error)

type Result[T any] struct {
	Value T
	Err error
}

type holder[T any] struct {
	job Job[T]
	resultCh chan Result[T]
}

type Option[T any] func(*Pool[T])

// New spawns workers that select between receiving a Holder
// or context cancellation
func New[T any](ctx context.Context, workerCount int, opts ...Option[T]) *Pool[T] {
	if workerCount < 1 {
		workerCount = 1
	}
	
	p := &Pool[T]{}
	p.BufferSize = 10 // default
	for _, opt := range opts {
		opt(p)
	}
	p.workerChan = make(chan holder[T], p.BufferSize)

	p.workerWait.Add(workerCount)
	for range workerCount {
		go func() {
			for {
				select {
				case h, ok := <-p.workerChan:
					if !ok {
						p.workerWait.Done()
						return
					}
					safeRun(h.job, h.resultCh, &p.stats)

				case <-ctx.Done():
					// drain remaining jobs so callers aren't left blocking forever
					for {
						select {
						case h, ok := <-p.workerChan:
							if !ok {
								p.workerWait.Done()
								return
							}
							h.resultCh <- Result[T]{Err: ctx.Err()}

						default:
							p.workerWait.Done()
							return
						}
					}
				}
			}
		}()
	}

	return p
}

// packs the job and result channel into a Holder and select
// between sending it or context cancellation
func (p *Pool[T]) Submit(ctx context.Context, job Job[T]) (<-chan Result[T], error) {
	resultChan := make(chan Result[T], 1)
	h := holder[T]{
		job: job,
		resultCh: resultChan,
	}

	select {
	case p.workerChan <- h:
		
	case <-ctx.Done():
		return nil, ctx.Err() 
	}

	return resultChan, nil
}

// Stop stops the workers after completing their jobs
func (p *Pool[T]) Stop() {
	close(p.workerChan)
	p.workerWait.Wait()
}

// WithBufferSize sets the job channel buffer size
func WithBufferSize[T any](size int) Option[T] {
	return func(p *Pool[T]) {
		if size > 0 {
			p.BufferSize = size
		}
	}
}

// Stats returns a snapshot of current pool activity
func (p *Pool[T]) Stats() StatsSnapshot {
	return StatsSnapshot{
		Queued: int64(len(p.workerChan)),
		Running: p.stats.Running.Load(),
	}
}

// helper func that defers a recovery, executes the job, and
// send the result either way, panic or success
func safeRun[T any](job Job[T], resultCh chan<- Result[T], stats *stats) {
	stats.Running.Add(1)
	defer stats.Running.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			// a panic happened, r is whatever was passed to panic()
			resultCh <- Result[T]{Err: fmt.Errorf("job panicked: %v", r)}
		}
	}()

	// if job panics, the deferred func above catches it
	val, err := job()
	resultCh <- Result[T]{Value: val, Err: err}
}