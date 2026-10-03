package dashboard

import (
	"context"
	"html/template"
	"net"
	"net/http"
	"net/netip"
	"path"
	"strings"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
)

// Access is what a request may do, as decided by [Options.Authorize].
type Access uint8

const (
	Denied    Access = iota // nothing: the request gets a 403
	ReadOnly                // pages and API reads
	ReadWrite               // also requeue, delete, trigger, pause and resume
)

// Part says what [Options.Redact] is given.
type Part uint8

const (
	PartArgs   Part = iota // the args of a job, or of a recurring job's template
	PartMeta               // the meta of a job or a batch, as a JSON object
	PartOutput             // the output of a job
	PartError              // an error or a stack trace from a job's history, as plain text
)

// String returns "args", "meta", "output" or "error".
func (p Part) String() string {
	switch p {
	case PartArgs:
		return "args"
	case PartMeta:
		return "meta"
	case PartOutput:
		return "output"
	case PartError:
		return "error"
	}
	return "unknown"
}

// Options configures the handler returned by [New].
type Options struct {
	// Prefix is the path the dashboard is served under, such as "/kiln". Links in the pages start
	// with it, and it is removed from request paths, so the handler needs no http.StripPrefix.
	Prefix string

	// Authorize decides the access of each request. It is required; [AllowAll] is enough for
	// local development.
	Authorize func(*http.Request) Access

	// Redact, when set, rewrites what the dashboard shows of a job's args, meta, output and
	// history, in pages and API alike. kind is the job's kind, empty for the meta of a batch. A
	// result for args, meta or output that is not valid JSON is shown as a JSON string.
	Redact func(kind string, part Part, v []byte) []byte

	// Actor, when set, returns the name of the user behind a request, shown in the page header.
	Actor func(*http.Request) string

	// Title names the application in the header and in page titles. Empty means "kiln".
	Title string
}

// AllowAll grants [ReadWrite] access to requests addressed to localhost or to a loopback address,
// and denies the others. It is meant for local development: it trusts the Host header, which the
// client sets, so it protects nothing on a listener that other machines can reach.
func AllowAll(r *http.Request) Access {
	if loopback(r.Host) {
		return ReadWrite
	}
	return Denied
}

func loopback(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(strings.Trim(host, "[]"))
	return err == nil && ip.Unmap().IsLoopback()
}

type handler struct {
	c      *kiln.Client
	store  driver.Store
	lr     driver.LimitReader
	opt    Options
	prefix string
	mux    http.ServeMux
	tmpl   map[string]*template.Template

	stats *memo[*snapshot]
	hour  *memo[*chart]
	day   *memo[*chart]
}

// New returns a handler that serves the dashboard for c's store. It panics if o.Authorize is nil.
func New(c *kiln.Client, o Options) http.Handler {
	if o.Authorize == nil {
		panic("dashboard: Options.Authorize is nil")
	}
	if o.Title == "" {
		o.Title = "kiln"
	}
	h := &handler{
		c:      c,
		store:  c.Store(),
		opt:    o,
		prefix: strings.TrimRight(o.Prefix, "/"),
	}
	h.lr, _ = h.store.(driver.LimitReader)
	if h.prefix != "" && h.prefix[0] != '/' {
		h.prefix = "/" + h.prefix
	}
	h.stats = newMemo(2*time.Second, h.loadOverview)
	h.hour = newMemo(2*time.Second, func(ctx context.Context) (*chart, error) {
		return h.loadSeries(ctx, time.Hour, time.Minute)
	})
	h.day = newMemo(30*time.Second, func(ctx context.Context) (*chart, error) {
		return h.loadSeries(ctx, 24*time.Hour, 30*time.Minute)
	})
	h.tmpl = h.parse()
	h.routes()
	return h
}

func (h *handler) routes() {
	m := &h.mux
	m.HandleFunc("GET /{$}", h.overview)
	m.HandleFunc("GET /jobs", h.jobsIndex)
	m.HandleFunc("GET /assets/{name}", serveAsset)
	m.HandleFunc("GET /api/overview", h.overview)
	m.HandleFunc("GET /api/series", h.series)
	m.HandleFunc("GET /api/jobs/{id}/logs", h.logs)
	for _, p := range []string{"", "/api"} {
		m.HandleFunc("GET "+p+"/jobs/{key}", h.jobs)
		m.HandleFunc("GET "+p+"/retries", h.retries)
		m.HandleFunc("GET "+p+"/recurring", h.recurring)
		m.HandleFunc("GET "+p+"/queues", h.queues)
		m.HandleFunc("GET "+p+"/servers", h.servers)
		m.HandleFunc("GET "+p+"/batches", h.batches)
		m.HandleFunc("GET "+p+"/batches/{id}", h.batch)
		if h.lr != nil {
			m.HandleFunc("GET "+p+"/limits", h.limits)
		}
		m.HandleFunc("POST "+p+"/jobs/{op}", h.bulk)
		m.HandleFunc("POST "+p+"/jobs/{id}/{op}", h.jobAction)
		m.HandleFunc("POST "+p+"/recurring/{id}/{op}", h.recurringAction)
		m.HandleFunc("POST "+p+"/queues/{name}/{op}", h.queueAction)
	}
}

type accessKey struct{}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	secure(w.Header())
	a := h.opt.Authorize(r)
	if a != ReadOnly && a != ReadWrite {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	p, ok := h.inner(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		if code := guard(r, a, strings.HasPrefix(p, "/api/")); code != 0 {
			http.Error(w, http.StatusText(code), code)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	}
	r2 := r.WithContext(context.WithValue(r.Context(), accessKey{}, a))
	u := *r.URL
	u.Path, u.RawPath = p, ""
	r2.URL = &u
	h.mux.ServeHTTP(w, r2)
}

func (h *handler) inner(p string) (string, bool) {
	if h.prefix != "" && (p == h.prefix || strings.HasPrefix(p, h.prefix+"/")) {
		p = p[len(h.prefix):]
	}
	if p == "" || p[0] != '/' {
		p = "/" + p
	}
	return p, path.Clean(p) == p
}

func writable(r *http.Request) bool {
	a, _ := r.Context().Value(accessKey{}).(Access)
	return a == ReadWrite
}

func isAPI(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/api/")
}
