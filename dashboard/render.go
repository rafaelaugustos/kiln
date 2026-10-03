package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/rafaelaugustos/kiln/driver"
)

//go:embed templates
var templateFS embed.FS

var views = map[string]string{
	"overview":  "overview",
	"jobs":      "jobs",
	"retries":   "jobs",
	"job":       "job",
	"recurring": "recurring",
	"queues":    "queues",
	"servers":   "servers",
	"batches":   "batches",
	"batch":     "batch",
	"limits":    "limits",
	"error":     "error",
}

var bufs = sync.Pool{New: func() any { return new(bytes.Buffer) }}

type page struct {
	App   string
	Nav   []navItem
	Flash *flash
	Actor string
	Write bool
	CSS   string
	JS    string
	Icon  string
	Text  template.JS
	Query []param
	Data  any
}

type param struct {
	Name  string
	Value string
}

type navItem struct {
	Label string
	URL   string
	Key   string
	Count int64
	On    bool
	Alert bool
}

type flash struct {
	Text string
	URL  string
}

type errorView struct {
	Code    int
	Message string
}

func (h *handler) parse() map[*locale]map[string]*template.Template {
	out := make(map[*locale]map[string]*template.Template, len(locales))
	for _, lc := range locales {
		base := template.Must(template.New("").Funcs(h.funcs(lc)).ParseFS(templateFS, "templates/layout.html"))
		set := make(map[string]*template.Template, len(views))
		for v, file := range views {
			set[v] = template.Must(template.Must(base.Clone()).ParseFS(templateFS, "templates/"+file+".html"))
		}
		out[lc] = set
	}
	return out
}

func (h *handler) funcs(lc *locale) template.FuncMap {
	return template.FuncMap{
		"link":    h.link,
		"initial": initial,
		"iso":     iso,
		"clock":   clock,
		"preview": preview,
		"pretty":  pretty,
		"query":   url.QueryEscape,
		"num":     lc.num,
		"short":   lc.short,
		"ago":     lc.ago,
		"abs":     lc.abs,
		"stamp":   lc.stamp,
		"dur":     lc.dur,
		"label":   lc.label,
		"t": func(key string, args ...any) string {
			return lc.text(key, strs(args)...)
		},
		"tn": func(key string, n any, args ...any) string {
			return lc.plural(key, integer(n), strs(args)...)
		},
		"parts": func(key string) message { return lc.msgs[key] },
		"lang":  func() string { return lc.Tag },
		"langs": func() []*locale { return locales },
	}
}

func (h *handler) link(parts ...any) string {
	var b strings.Builder
	b.WriteString(h.prefix)
	for _, p := range parts {
		fmt.Fprint(&b, p)
	}
	if b.Len() == 0 {
		return "/"
	}
	return b.String()
}

func (h *handler) show(w http.ResponseWriter, r *http.Request, view string, data any) {
	if isAPI(r) {
		writeJSON(w, http.StatusOK, data)
		return
	}
	h.render(w, r, http.StatusOK, view, data)
}

func (h *handler) render(w http.ResponseWriter, r *http.Request, code int, view string, data any) {
	lc := h.locale(r)
	s, _ := h.stats.get(r.Context())
	q := r.URL.Query()
	p := &page{
		App:   h.opt.Title,
		Nav:   h.nav(view, s),
		Flash: h.flash(lc, q),
		Write: writable(r),
		CSS:   h.link("/assets/", assets["app.css"]),
		JS:    h.link("/assets/", assets["app.js"]),
		Icon:  h.link("/assets/", assets["icon.svg"]),
		Text:  lc.scripts[view],
		Data:  data,
	}
	for _, k := range slices.Sorted(maps.Keys(q)) {
		if k == "done" || k == "n" {
			continue
		}
		for _, v := range q[k] {
			p.Query = append(p.Query, param{k, v})
		}
	}
	if h.opt.Actor != nil {
		p.Actor = h.opt.Actor(r)
	}
	b := bufs.Get().(*bytes.Buffer)
	defer func() {
		if b.Cap() <= 1<<20 {
			b.Reset()
			bufs.Put(b)
		}
	}()
	if err := h.tmpl[lc][view].ExecuteTemplate(b, "layout", p); err != nil {
		http.Error(w, "kiln: render "+view+": "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	w.Write(b.Bytes())
}

func (h *handler) nav(view string, s *snapshot) []navItem {
	section := view
	switch view {
	case "job":
		section = "jobs"
	case "batch":
		section = "batches"
	}
	items := []navItem{
		{Label: "nav.overview", URL: h.link("/"), On: section == "overview"},
		{Label: "nav.jobs", URL: h.link("/jobs/", driver.Enqueued), Key: "failed", Alert: true, On: section == "jobs"},
		{Label: "nav.retries", URL: h.link("/retries"), Key: "retries", On: section == "retries"},
		{Label: "nav.recurring", URL: h.link("/recurring"), Key: "recurring", On: section == "recurring"},
		{Label: "nav.queues", URL: h.link("/queues"), Key: "queues", On: section == "queues"},
		{Label: "nav.servers", URL: h.link("/servers"), Key: "servers", On: section == "servers"},
		{Label: "nav.batches", URL: h.link("/batches"), On: section == "batches"},
	}
	if h.lr != nil {
		items = append(items, navItem{Label: "nav.limits", URL: h.link("/limits"), On: section == "limits"})
	}
	if s != nil {
		for i := range items {
			items[i].Count = s.count(items[i].Key)
		}
	}
	return items
}

func (h *handler) flash(lc *locale, q url.Values) *flash {
	n, _ := strconv.ParseInt(q.Get("n"), 10, 64)
	switch q.Get("done") {
	case "requeued":
		return &flash{Text: lc.plural("flash.requeued", n)}
	case "deleted":
		return &flash{Text: lc.plural("flash.deleted", n)}
	case "triggered":
		return &flash{Text: lc.text("flash.triggered", "id", strconv.FormatInt(n, 10)), URL: h.link("/jobs/", n)}
	case "queue-paused":
		return &flash{Text: lc.text("flash.queue_paused")}
	case "queue-resumed":
		return &flash{Text: lc.text("flash.queue_resumed")}
	case "recurring-paused":
		return &flash{Text: lc.text("flash.recurring_paused")}
	case "recurring-resumed":
		return &flash{Text: lc.text("flash.recurring_resumed")}
	case "recurring-removed":
		return &flash{Text: lc.text("flash.recurring_removed")}
	}
	return nil
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, driver.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, driver.ErrInvalid), errors.Is(err, driver.ErrTooLarge):
		code = http.StatusBadRequest
	case errors.Is(err, driver.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, context.Canceled):
		return
	}
	if isAPI(r) {
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	h.render(w, r, code, "error", &errorView{Code: code, Message: err.Error()})
}

func (h *handler) redirect(w http.ResponseWriter, r *http.Request, target string, q url.Values) {
	u := h.link(target)
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	http.Redirect(w, r, u, http.StatusSeeOther)
}

var past = map[string]string{
	"requeue": "requeued",
	"delete":  "deleted",
	"pause":   "paused",
	"resume":  "resumed",
	"remove":  "removed",
}

func done(op string, n int64) url.Values {
	return url.Values{"done": {op}, "n": {strconv.FormatInt(n, 10)}}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func decode(r *http.Request, v any) error {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: body: %v", driver.ErrInvalid, err)
	}
	return nil
}
