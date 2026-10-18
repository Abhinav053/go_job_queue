package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	startupCtx, cancelStartup := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	db, err := openDB(startupCtx)
	cancelStartup()

	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer db.Close()

	workerCtx, cancelWorkers := context.WithCancel(
		context.Background(),
	)
	var workers sync.WaitGroup

	for i := 1; i <= 3; i++ {
		workers.Add(1)
		go workerLoop(workerCtx, i, db, &workers)
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		recoveryLoop(workerCtx, db)
	}()

	api := NewAPI(db)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)

	go func() {
		log.Println("API listening on :8080")
		serverErr <- server.ListenAndServe()
	}()

	stop, stopSignal := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignal()

	select {
	case <-stop.Done():
		log.Println("Shutdown signal received")
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}

	httpCtx, cancelHTTP := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	if err := server.Shutdown(httpCtx); err != nil {
		log.Printf("HTTP shutdown: %v", err)
		_ = server.Close()
	}
	cancelHTTP()

	cancelWorkers()
	workers.Wait()

	log.Println("Server stopped")
}
