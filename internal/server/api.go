package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"vigil/internal/engine"
	"vigil/internal/probeapi"
)

var probeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (s *server) registerProbeAPI(mux *http.ServeMux) {
	mux.Handle("POST "+probeapi.LeasePath, s.probeAuth(http.HandlerFunc(s.lease)))
	mux.Handle("POST "+probeapi.ReportPath, s.probeAuth(http.HandlerFunc(s.report)))
}

func (s *server) probeAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.opts.ProbeToken == "" {
			http.Error(w, "remote probes are disabled: set VIGIL_PROBE_TOKEN on the server", http.StatusForbidden)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.ProbeToken)) != 1 {
			http.Error(w, "invalid probe token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func decode(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	if err := dec.Decode(v); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func validProbe(w http.ResponseWriter, id, region string) bool {
	if !probeIDPattern.MatchString(id) || !probeIDPattern.MatchString(region) {
		http.Error(w, "probe_id and region must be 1-64 characters of letters, digits, '.', '_' or '-'", http.StatusBadRequest)
		return false
	}
	return true
}

func (s *server) lease(w http.ResponseWriter, r *http.Request) {
	var req probeapi.LeaseRequest
	if !decode(w, r, 64<<10, &req) || !validProbe(w, req.ProbeID, req.Region) {
		return
	}
	resp, err := s.engine.Lease(r.Context(), req, r.RemoteAddr, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

func (s *server) report(w http.ResponseWriter, r *http.Request) {
	var req probeapi.ReportRequest
	if !decode(w, r, 8<<20, &req) || !validProbe(w, req.ProbeID, req.Region) {
		return
	}
	resp, err := s.engine.Report(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// localAPI lets the embedded probe call the engine directly. It is the same
// protocol as the HTTP API minus serialization, so the embedded probe
// exercises exactly the code paths a remote one does.
type localAPI struct{ e *engine.Engine }

func (l localAPI) Lease(ctx context.Context, req probeapi.LeaseRequest) (probeapi.LeaseResponse, error) {
	return l.e.Lease(ctx, req, "in-process", true)
}

func (l localAPI) Report(ctx context.Context, req probeapi.ReportRequest) (probeapi.ReportResponse, error) {
	return l.e.Report(ctx, req)
}
