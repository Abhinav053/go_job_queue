package main

import (
	"context"
	"time"
)

type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusRetrying  JobStatus = "retrying"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
)

type Job struct {
	ID             int64     `json:"id"`
	Payload        string    `json:"payload"`
	Status         JobStatus `json:"status"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	TimeoutSeconds int       `json:"timeout_seconds"`
	RunAt          time.Time `json:"run_at"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type JobHandler func(context.Context, Job) error
