package health

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"hros-event-worker/internal/kafka"
	"hros-event-worker/internal/outbox"
)

type Server struct {
	httpServer *http.Server
	repo       outbox.Repository
	publisher  kafka.Publisher
}

func NewServer(port int, repo outbox.Repository, publisher kafka.Publisher) *Server {
	if port <= 0 {
		port = 8080
	}

	mux := http.NewServeMux()
	s := &Server{
		repo:      repo,
		publisher: publisher,
	}

	mux.HandleFunc("/healthz", s.handleLiveness)
	mux.HandleFunc("/readyz", s.handleReadiness)
	mux.Handle("/metrics", promhttp.Handler())

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if s.repo != nil {
		if err := s.repo.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf(`{"status":"DOWN","reason":"database unreachable: %s"}`, err.Error()), http.StatusServiceUnavailable)
			return
		}
	}

	if s.publisher != nil {
		if err := s.publisher.Ping(ctx); err != nil {
			http.Error(w, fmt.Sprintf(`{"status":"DOWN","reason":"kafka unreachable: %s"}`, err.Error()), http.StatusServiceUnavailable)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"READY"}`))
}
