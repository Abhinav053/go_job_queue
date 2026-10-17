package main

import "context"

type Job struct {
	ID      int
	Payload string
}

type JobHandler func(context.Context, Job) error
