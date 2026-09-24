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

type Access uint8

const (
	Denied Access = iota
	ReadOnly
	ReadWrite
)

type Part uint8

const (
	PartArgs Part = iota
	PartMeta
	PartOutput
	PartError
)

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

type Options struct {
	Prefix    string
	Authorize func(*http.Request) Access
	Redact    func(kind string, part Part, v []byte) []byte
	Actor     func(*http.Request) string
	Title     string
}

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
	opt    Options
	prefix string
	mux    http.ServeMux
	tmpl   map[string]*template.Template

	stats *memo[*snapshot]
	hour  *memo[*chart]
	day   *memo[*chart]
}

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
	for _, p := range []string{"", "/api"} {
		m.HandleFunc("GET "+p+"/jobs/{key}", h.jobs)
		m.HandleFunc("GET "+p+"/retries", h.retries)
		m.HandleFunc("GET "+p+"/recurring", h.recurring)
		m.HandleFunc("GET "+p+"/queues", h.queues)
		m.HandleFunc("GET "+p+"/servers", h.servers)
		m.HandleFunc("GET "+p+"/batches", h.batches)
		m.HandleFunc("GET "+p+"/batches/{id}", h.batch)
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
