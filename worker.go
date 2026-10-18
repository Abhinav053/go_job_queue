package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func claimJob(ctx context.Context, db *pgxpool.Pool) (Job, error) {
	const query = `
        WITH candidate AS (
            SELECT id
            FROM jobs
            WHERE status IN ('queued', 'retrying')
              AND run_at <= NOW()
            ORDER BY run_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE jobs AS j
        SET status = 'running',
            attempts = j.attempts + 1,
            locked_at = NOW(),
            updated_at = NOW()
        FROM candidate
        WHERE j.id = candidate.id
        RETURNING j.id, j.payload, j.status, j.attempts,
                  j.max_attempts, j.timeout_seconds, j.run_at,
                  COALESCE(j.last_error, ''), j.created_at, j.updated_at
    `

	var job Job
	err := db.QueryRow(ctx, query).Scan(
		&job.ID,
		&job.Payload,
		&job.Status,
		&job.Attempts,
		&job.MaxAttempts,
		&job.TimeoutSeconds,
		&job.RunAt,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	return job, err
}

func updateJob(
	ctx context.Context,
	db *pgxpool.Pool,
	id int64,
	status JobStatus,
	runAt time.Time,
	jobErr error,
) error {
	var message *string
	if jobErr != nil {
		s := jobErr.Error()
		message = &s
	}

	_, err := db.Exec(ctx, `
        UPDATE jobs
        SET status = $2,
            run_at = $3,
            last_error = $4,
            locked_at = NULL,
            updated_at = NOW()
        WHERE id = $1
    `, id, status, runAt, message)

	return err
}

// Execute a demonstration job. Replace this with real job handlers later.
func executeJob(ctx context.Context, job Job) error {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	// A payload of "fail" simulates a persistent failure.
	if strings.TrimSpace(job.Payload) == "fail" {
		return errors.New("simulated processing failure")
	}

	log.Printf("Processed job %d: %s", job.ID, job.Payload)
	return nil
}

func processJob(
	rootCtx context.Context,
	db *pgxpool.Pool,
	job Job,
) {
	jobCtx, cancel := context.WithTimeout(
		rootCtx,
		time.Duration(job.TimeoutSeconds)*time.Second,
	)
	err := executeJob(jobCtx, job)
	cancel()

	// Use a separate bounded context to persist the result.
	saveCtx, saveCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer saveCancel()

	if rootCtx.Err() != nil {
		// Leave the job recoverable if the worker is stopping.
		err = updateJob(
			saveCtx, db, job.ID, StatusQueued,
			time.Now().UTC(), errors.New("worker shutting down"),
		)
		if err != nil {
			log.Printf("Could not release job %d: %v", job.ID, err)
		}
		return
	}

	if err == nil {
		err = updateJob(
			saveCtx, db, job.ID, StatusCompleted,
			job.RunAt, nil,
		)
		if err != nil {
			log.Printf("Could not complete job %d: %v", job.ID, err)
		}
		return
	}

	if job.Attempts >= job.MaxAttempts {
		if saveErr := updateJob(
			saveCtx, db, job.ID, StatusFailed,
			job.RunAt, err,
		); saveErr != nil {
			log.Printf("Could not fail job %d: %v", job.ID, saveErr)
		}
		return
	}

	// Exponential backoff, capped at 256 seconds.
	exponent := job.Attempts - 1
	if exponent < 0 {
		exponent = 0
	}
	if exponent > 8 {
		exponent = 8
	}

	delay := time.Second * time.Duration(1<<exponent)
	retryAt := time.Now().UTC().Add(delay)

	if saveErr := updateJob(
		saveCtx, db, job.ID, StatusRetrying,
		retryAt, err,
	); saveErr != nil {
		log.Printf("Could not schedule retry for job %d: %v",
			job.ID, saveErr)
	}
}

func recoverStaleJobs(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
        UPDATE jobs
        SET status = CASE
                WHEN attempts >= max_attempts THEN 'failed'
                ELSE 'retrying'
            END,
            run_at = NOW(),
            last_error = 'worker lease expired; recovered',
            locked_at = NULL,
            updated_at = NOW()
        WHERE status = 'running'
          AND locked_at < NOW() - INTERVAL '10 minutes'
    `)

	return err
}

func recoveryLoop(ctx context.Context, db *pgxpool.Pool) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		if err := recoverStaleJobs(ctx, db); err != nil && ctx.Err() == nil {
			log.Printf("Job recovery failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func workerLoop(
	ctx context.Context,
	workerID int,
	db *pgxpool.Pool,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	log.Printf("Worker %d started", workerID)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		job, err := claimJob(ctx, db)

		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// No eligible jobs currently exist.
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			log.Printf("Worker %d claim error: %v", workerID, err)
		default:
			log.Printf("Worker %d claimed job %d",
				workerID, job.ID)
			processJob(ctx, db, job)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
