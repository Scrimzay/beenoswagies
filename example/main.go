package main

import (
	"context"
	//"crypto/sha256"
	"fmt"
	"time"

	worker "github.com/Scrimzay/beenoswagies"
)

func main() {
    ctx := context.Background()
    pool := worker.New[int](ctx, 1, worker.WithBufferSize[int](20))

    var results []<-chan worker.Result[int]
    for i := 0; i < 10; i++ {
        ch, err := pool.Submit(ctx, func() (int, error) {
            time.Sleep(500 * time.Millisecond)
            return 1, nil
        })
        if err != nil {
            fmt.Printf("submit error: %v\n", err)
            continue
        }
        results = append(results, ch)
    }

    // snapshot mid-flight before jobs finish
    time.Sleep(100 * time.Millisecond)
    stats := pool.Stats()
    fmt.Printf("queued: %d, running: %d\n", stats.Queued, stats.Running)

    // drain all results
    total := 0
    for _, ch := range results {
        r := <-ch
        if r.Err != nil {
            fmt.Println("error:", r.Err)
            continue
        }
        total += r.Value
    }

    // stats should be zeroed out now
    stats = pool.Stats()
    fmt.Printf("after drain — queued: %d, running: %d\n", stats.Queued, stats.Running)
    fmt.Printf("total: %d\n", total)

    pool.Stop()
}