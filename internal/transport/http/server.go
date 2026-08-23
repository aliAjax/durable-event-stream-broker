package httptransport

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/service"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	Addr   string
	Broker *service.Broker
	http   *http.Server
}

func New(addr string, b *service.Broker) *Server {
	s := &Server{Addr: addr, Broker: b}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/readyz", s.health)
	mux.HandleFunc("/debug/pprof/", s.pprof)
	mux.HandleFunc("/api/v1/tenants", s.tenants)
	mux.HandleFunc("/api/v1/streams", s.streams)
	mux.HandleFunc("/api/v1/topics/", s.topic)
	mux.HandleFunc("/api/v1/consumer-groups/", s.groups)
	s.http = &http.Server{Addr: addr, Handler: logging(mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	return s
}
func (s *Server) Start() error                   { return s.http.ListenAndServe() }
func (s *Server) Stop(ctx context.Context) error { return s.http.Shutdown(ctx) }
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	write(w, 200, map[string]any{"status": "ok", "version": "v1"})
}
func (s *Server) pprof(w http.ResponseWriter, r *http.Request) {
	write(w, 200, map[string]any{"goroutines": runtime.NumGoroutine()})
}

type tenantReq struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	QuotaBytes int64  `json:"quota_bytes"`
}

func (s *Server) tenants(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		write(w, 200, map[string]any{"items": s.Broker.Repo.Tenants})
		return
	}
	var in tenantReq
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		problem(w, 400, domain.ErrInvalid, err.Error())
		return
	}
	if in.ID == "" {
		problem(w, 400, domain.ErrInvalid, "id required")
		return
	}
	if err := s.Broker.CreateTenant(in.ID, in.Name, in.QuotaBytes); err != nil {
		problemErr(w, err)
		return
	}
	write(w, 201, in)
}

type streamReq struct {
	TenantID   string `json:"tenant_id"`
	ID         string `json:"id"`
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

func (s *Server) streams(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		write(w, 200, map[string]any{"items": s.Broker.Repo.ListTopics(r.URL.Query().Get("tenant_id"))})
		return
	}
	var in streamReq
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		problem(w, 400, domain.ErrInvalid, "invalid body")
		return
	}
	t, err := s.Broker.CreateTopic(in.TenantID, in.ID, in.Name, in.Partitions)
	if err != nil {
		problemErr(w, err)
		return
	}
	write(w, 201, t)
}
func (s *Server) topic(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/topics/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		problem(w, 404, domain.ErrNotFound, "endpoint")
		return
	}
	id := parts[0]
	if parts[1] == "records:append" && r.Method == http.MethodPost {
		s.append(w, r, id)
		return
	}
	if parts[1] == "records" && r.Method == http.MethodGet {
		s.fetch(w, r, id)
		return
	}
	problem(w, 404, domain.ErrNotFound, "endpoint")
}

type appendReq struct {
	TenantID   string          `json:"tenant_id"`
	Partition  int             `json:"partition"`
	ProducerID string          `json:"producer_id"`
	Sequence   int64           `json:"sequence"`
	Records    []domain.Record `json:"records"`
}

func (s *Server) append(w http.ResponseWriter, r *http.Request, id string) {
	var in appendReq
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		problem(w, 400, domain.ErrInvalid, "invalid body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	out, err := s.Broker.Append(ctx, in.TenantID, id, in.Partition, in.Records, in.ProducerID, in.Sequence, r.Header.Get("Idempotency-Key"))
	if err != nil {
		problemErr(w, err)
		return
	}
	write(w, 200, map[string]any{"records": out, "cursor": service.Cursor(id, in.Partition, out[len(out)-1].Offset)})
}
func (s *Server) fetch(w http.ResponseWriter, r *http.Request, id string) {
	part, _ := strconv.Atoi(r.URL.Query().Get("partition"))
	after, _ := strconv.ParseInt(r.URL.Query().Get("after_offset"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	out, err := s.Broker.Fetch(id, part, domain.Offset(after), limit)
	if err != nil {
		problemErr(w, err)
		return
	}
	write(w, 200, map[string]any{"records": out})
}
func (s *Server) groups(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/consumer-groups/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		problem(w, 404, domain.ErrNotFound, "endpoint")
		return
	}
	g := s.Broker.Repo.EnsureGroup(parts[0], r.URL.Query().Get("topic_id"))
	switch parts[1] {
	case "join":
		g.Join(r.URL.Query().Get("member_id"), time.Now())
		write(w, 200, g)
	case "heartbeat":
		if err := g.Heartbeat(r.URL.Query().Get("member_id"), time.Now()); err != nil {
			problemErr(w, err)
			return
		}
		write(w, 200, g)
	case "pause":
		g.Pause()
		write(w, 200, g)
	case "resume":
		g.Resume()
		write(w, 200, g)
	case "offsets":
		var in struct {
			Partition int
			Offset    int64
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil {
			problem(w, 400, domain.ErrInvalid, "invalid body")
			return
		}
		if err := g.Commit(in.Partition, domain.Offset(in.Offset)); err != nil {
			problemErr(w, err)
			return
		}
		write(w, 200, g)
	default:
		problem(w, 404, domain.ErrNotFound, "endpoint")
	}
}
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("method=%s path=%s duration=%s\n", r.Method, r.URL.Path, time.Since(start))
	})
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, code domain.ErrorCode, msg string) {
	write(w, status, map[string]any{"error": map[string]string{"code": string(code), "message": msg}})
}
func problemErr(w http.ResponseWriter, err error) {
	var e *domain.BrokerError
	if direct, ok := err.(*domain.BrokerError); ok {
		e = direct
		problem(w, codeStatus(e.Code), e.Code, e.Message)
		return
	}
	problem(w, 500, domain.ErrUnavailable, err.Error())
}
func codeStatus(c domain.ErrorCode) int {
	switch c {
	case domain.ErrInvalid:
		return 400
	case domain.ErrNotFound:
		return 404
	case domain.ErrConflict:
		return 409
	case domain.ErrQuota:
		return 429
	case domain.ErrFenced:
		return 409
	default:
		return 503
	}
}
