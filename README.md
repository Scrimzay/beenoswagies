# worker

A generic, concurrent worker pool library for Go with context cancellation, panic recovery, and runtime metrics.

---

## Requirements

- Go 1.22+

---

## Installation

```bash
go get github.com/Scrimzay/beenoswagies
```

---

## Concepts

| Term | Description |
|------|-------------|
| `Pool[T]` | A pool of workers that process jobs returning type `T` |
| `Job[T]` | A function `func() (T, error)` submitted to the pool |
| `Result[T]` | A struct holding the job's return value and any error |
| `Option[T]` | A functional option used to configure the pool at creation |

---

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/Scrimzay/beenoswagies"
)

func main() {
    ctx := context.Background()

    // create a pool with 3 workers
    pool := worker.New[int](ctx, 3)
    defer pool.Stop()

    // submit a job — returns a result channel
    ch, err := pool.Submit(ctx, func() (int, error) {
        return 42, nil
    })
    if err != nil {
        fmt.Println("submit failed:", err)
        return
    }

    // block until the result is ready
    result := <-ch
    if result.Err != nil {
        fmt.Println("job failed:", result.Err)
        return
    }

    fmt.Println("result:", result.Value) // result: 42
}
```

---

## Submitting Multiple Jobs

```go
pool := worker.New[int](ctx, 5)
defer pool.Stop()

var results []<-chan worker.Result[int]

for i := 0; i < 20; i++ {
    start := i * 100
    ch, err := pool.Submit(ctx, func() (int, error) {
        sum := 0
        for j := start; j < start+100; j++ {
            sum += j
        }
        return sum, nil
    })
    if err != nil {
        fmt.Println("submit cancelled:", err)
        break
    }
    results = append(results, ch)
}

for _, ch := range results {
    r := <-ch
    fmt.Println(r.Value, r.Err)
}
```

---

## Context Cancellation

Pass a cancellable or timeout context to stop the pool mid-flight. Any pending jobs that haven't started will receive a cancellation error on their result channel rather than blocking forever.

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()

pool := worker.New[int](ctx, 3)
defer pool.Stop()

for i := 0; i < 100; i++ {
    ch, err := pool.Submit(ctx, func() (int, error) {
        time.Sleep(500 * time.Millisecond)
        return i, nil
    })
    if err != nil {
        // context was cancelled before this job could be submitted
        fmt.Println("submit cancelled at job", i, ":", err)
        break
    }
    go func() {
        r := <-ch
        if r.Err != nil {
            fmt.Println("job error:", r.Err)
        }
    }()
}
```

---

## Panic Recovery

If a job panics, the worker recovers gracefully, sends the panic as an error on the result channel, and continues processing future jobs. The pool is never taken down by a panicking job.

```go
ch, _ := pool.Submit(ctx, func() (int, error) {
    panic("something went wrong")
})

r := <-ch
fmt.Println(r.Err) // job panicked: something went wrong
```

---

## Options

Options are passed as variadic arguments to `New` to configure the pool without bloating the function signature.

### `WithBufferSize`

Controls how many jobs can be queued in the channel before `Submit` blocks. Defaults to `10`.

```go
pool := worker.New[int](ctx, 3, worker.WithBufferSize[int](100))
```

---

## Metrics

`Stats()` returns a snapshot of current pool activity. Safe to call from any goroutine at any time.

```go
snapshot := pool.Stats()
fmt.Println("queued:", snapshot.Queued)
fmt.Println("running:", snapshot.Running)
```

| Field | Description |
|-------|-------------|
| `Queued` | Jobs sitting in the channel waiting to be picked up |
| `Running` | Jobs currently executing across all workers |

---

## Stopping the Pool

`Stop()` closes the job channel and blocks until all active workers have exited. Always defer it or call it explicitly — not calling `Stop()` will leak goroutines.

```go
pool := worker.New[int](ctx, 3)
defer pool.Stop()
```

**Note:** `Stop()` and context cancellation are independent. Cancelling the context stops workers from picking up new jobs. `Stop()` closes the channel and waits for all workers to exit cleanly. For a clean shutdown, cancel the context first then call `Stop()`.

---

## API Reference

```go
// New creates a pool of workerCount goroutines ready to process jobs of type T.
func New[T any](ctx context.Context, workerCount int, opts ...Option[T]) *Pool[T]

// Submit enqueues a job and returns a channel that will receive exactly one Result.
// Returns an error if the context is cancelled before the job could be submitted.
func (p *Pool[T]) Submit(ctx context.Context, job Job[T]) (<-chan Result[T], error)

// Stop closes the job channel and waits for all workers to exit.
func (p *Pool[T]) Stop()

// Stats returns a point-in-time snapshot of pool activity.
func (p *Pool[T]) Stats() StatsSnapshot

// WithBufferSize sets the job channel buffer size. Must be greater than 0.
func WithBufferSize[T any](size int) Option[T]
```
