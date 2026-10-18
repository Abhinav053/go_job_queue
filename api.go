package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type API struct {
	db *pgxpool.Pool
}

func NewAPI(db *pgxpool.Pool) *API {
	return &API{db: db}
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", a.createJob)
	mux.HandleFunc("GET /jobs/{id}", a.getJob)
	mux.HandleFunc("GET /healthz", a.health)

	return mux
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Payload        string `json:"payload"`
		MaxAttempts    int    `json:"max_attempts"`
		TimeoutSeconds int    `json:"timeout_seconds"`
		RunAt          string `json:"run_at"`
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request"})
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request must contain one JSON object"})
		return
	}

	input.Payload = strings.TrimSpace(input.Payload)
	if input.Payload == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "payload is required"})
		return
	}
	if input.MaxAttempts == 0 {
		input.MaxAttempts = 3
	}
	if input.MaxAttempts < 1 || input.MaxAttempts > 5 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max_attempts must be between 1 and 5"})
		return
	}
	if input.TimeoutSeconds == 0 {
		input.TimeoutSeconds = 10
	}
	if input.TimeoutSeconds < 1 || input.TimeoutSeconds > 300 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "timeout_seconds must be between 1 and 300"})
		return
	}

	runAt := time.Now().UTC()
	if input.RunAt != "" {
		parsed, err := time.Parse(time.RFC3339, input.RunAt)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "run_at must be RFC3339"})
			return
		}
		if parsed.After(time.Now().Add(24 * time.Hour)) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "run_at cannot be more than 24 hours ahead"})
			return
		}
		runAt = parsed.UTC()
	}

	var job Job
	err := a.db.QueryRow(r.Context(), `
		INSERT INTO jobs (payload, max_attempts, timeout_seconds, run_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id, payload, status, attempts, max_attempts,
		          timeout_seconds, run_at, COALESCE(last_error, ''),
		          created_at, updated_at
	`, input.Payload, input.MaxAttempts, input.TimeoutSeconds, runAt).Scan(
		&job.ID, &job.Payload, &job.Status, &job.Attempts,
		&job.MaxAttempts, &job.TimeoutSeconds, &job.RunAt,
		&job.LastError, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create job"})
		return
	}

	w.Header().Set("Location", "/jobs/"+strconv.FormatInt(job.ID, 10))
	writeJSON(w, http.StatusAccepted, job)
}

func (a *API) getJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid job ID",
		})
		return
	}

	var job Job
	err = a.db.QueryRow(r.Context(), `
		SELECT id, payload, status, attempts, max_attempts,
		       timeout_seconds, run_at, COALESCE(last_error, ''),
		       created_at, updated_at
		FROM jobs WHERE id = $1
	`, id).Scan(
		&job.ID, &job.Payload, &job.Status, &job.Attempts,
		&job.MaxAttempts, &job.TimeoutSeconds, &job.RunAt,
		&job.LastError, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeJSON(w, http.StatusNotFound, map[string]string{
				"error": "job not found",
			})
			return
		}

		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "could not retrieve job",
		})
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (a *API) health(
	w http.ResponseWriter,
	r *http.Request,
) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}
