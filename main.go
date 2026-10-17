package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func main() {
	handler := func(
		ctx context.Context,
		job Job,
	) error {
		// Simulate work that takes one second.
		timer := time.NewTimer(time.Second)
		defer timer.Stop()

		select {
		case <-timer.C:
			if job.ID == 4 {
				return errors.New(
					"simulated processing error",
				)
			}

			fmt.Printf(
				"Processed payload: %s\n",
				job.Payload,
			)
			return nil

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	pool, err := NewWorkerPool(3, 5, handler)
	if err != nil {
		panic(err)
	}

	// Submit 10 jobs.
	for i := 1; i <= 10; i++ {
		job := Job{
			ID:      i,
			Payload: fmt.Sprintf("Task-%d", i),
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		err := pool.Submit(ctx, job)
		cancel()

		if err != nil {
			fmt.Printf(
				"Could not submit job %d: %v\n",
				job.ID,
				err,
			)
			continue
		}

		fmt.Printf("Submitted job %d\n", job.ID)
	}

	// Allow workers to finish their queued work.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := pool.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("Shutdown error: %v\n", err)
		return
	}

	fmt.Println("Worker pool shut down cleanly.")
}
