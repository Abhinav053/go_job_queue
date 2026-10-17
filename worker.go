package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type WorkerPool struct {
	jobs    chan Job
	handler JobHandler

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	// Protects queue closure against concurrent submissions.
	mu     sync.RWMutex
	closed bool
}

func NewWorkerPool(
	workerCount int,
	queueSize int,
	handler JobHandler,
) (*WorkerPool, error) {
	if workerCount <= 0 {
		return nil, errors.New("worker count must be positive")
	}

	if queueSize < 0 {
		return nil, errors.New("queue size cannot be negative")
	}

	if handler == nil {
		return nil, errors.New("handler cannot be nil")
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	pool := &WorkerPool{
		jobs:    make(chan Job, queueSize),
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := 1; i <= workerCount; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
	}

	return pool, nil
}

func (p *WorkerPool) Submit(
	ctx context.Context,
	job Job,
) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return errors.New("worker pool is closed")
	}

	select {
	case p.jobs <- job:
		return nil

	case <-ctx.Done():
		return ctx.Err()

	case <-p.ctx.Done():
		return errors.New("worker pool stopped")
	}
}

func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()

	for job := range p.jobs {
		if p.ctx.Err() != nil {
			return
		}

		fmt.Printf(
			"Worker %d started job %d\n",
			id,
			job.ID,
		)

		err := p.handler(p.ctx, job)
		if err != nil {
			fmt.Printf(
				"Worker %d: job %d failed: %v\n",
				id,
				job.ID,
				err,
			)
			continue
		}

		fmt.Printf(
			"Worker %d completed job %d\n",
			id,
			job.ID,
		)
	}
}

func (p *WorkerPool) Shutdown(
	ctx context.Context,
) error {
	p.mu.Lock()

	if !p.closed {
		p.closed = true
		close(p.jobs)
	}

	p.mu.Unlock()

	done := make(chan struct{})

	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.cancel()
		return nil

	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}
