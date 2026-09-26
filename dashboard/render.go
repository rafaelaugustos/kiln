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
	"net/http"
	"net/url"
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
	"error":     "error",
}

var bufs = sync.Pool{New: func() any { return new(bytes.Buffer) }}

type page struct {
	Title string
	App   string
	Nav   []navItem
	Flash *flash
	Actor string
	Write bool
	CSS   string
	JS    string
	Icon  string
	Data  any
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

func (h *handler) parse() map[string]*template.Template {
	base := template.Must(template.New("").Funcs(template.FuncMap{
		"link":    h.link,
		"num":     num,
		"ago":     ago,
		"abs":     abs,
		"stamp":   stamp,
		"initial": initial,
		"iso":     iso,
		"dur":     dur,
		"label":   label,
		"preview": preview,
		"pretty":  pretty,
		"plural":  plural,
	}).ParseFS(templateFS, "templates/layout.html"))
	out := make(map[string]*template.Template, len(views))
	for v, file := range views {
		out[v] = template.Must(template.Must(base.Clone()).ParseFS(templateFS, "templates/"+file+".html"))
	}
	return out
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

func (h *handler) show(w http.ResponseWriter, r *http.Request, view, title string, data any) {
	if isAPI(r) {
		writeJSON(w, http.StatusOK, data)
		return
	}
	h.render(w, r, http.StatusOK, view, title, data)
}

func (h *handler) render(w http.ResponseWriter, r *http.Request, code int, view, title string, data any) {
	s, _ := h.stats.get(r.Context())
	p := &page{
		Title: title,
		App:   h.opt.Title,
		Nav:   h.nav(view, s),
		Flash: h.flash(r.URL.Query()),
		Write: writable(r),
		CSS:   h.link("/assets/", assets["app.css"]),
		JS:    h.link("/assets/", assets["app.js"]),
		Icon:  h.link("/assets/", assets["icon.svg"]),
		Data:  data,
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
	if err := h.tmpl[view].ExecuteTemplate(b, "layout", p); err != nil {
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
		{Label: "Overview", URL: h.link("/"), On: section == "overview"},
		{Label: "Jobs", URL: h.link("/jobs/", driver.Enqueued), Key: "failed", Alert: true, On: section == "jobs"},
		{Label: "Retries", URL: h.link("/retries"), Key: "retries", On: section == "retries"},
		{Label: "Recurring", URL: h.link("/recurring"), Key: "recurring", On: section == "recurring"},
		{Label: "Queues", URL: h.link("/queues"), Key: "queues", On: section == "queues"},
		{Label: "Servers", URL: h.link("/servers"), Key: "servers", On: section == "servers"},
		{Label: "Batches", URL: h.link("/batches"), On: section == "batches"},
	}
	if s != nil {
		for i := range items {
			items[i].Count = s.count(items[i].Key)
		}
	}
	return items
}

func (h *handler) flash(q url.Values) *flash {
	n, _ := strconv.ParseInt(q.Get("n"), 10, 64)
	jobs := func(verb string) *flash {
		if n == 1 {
			return &flash{Text: verb + " 1 job"}
		}
		return &flash{Text: verb + " " + num(n) + " jobs"}
	}
	switch q.Get("done") {
	case "requeued":
		return jobs("Requeued")
	case "deleted":
		return jobs("Deleted")
	case "triggered":
		return &flash{Text: "Enqueued job " + strconv.FormatInt(n, 10), URL: h.link("/jobs/", n)}
	case "queue-paused":
		return &flash{Text: "Queue paused"}
	case "queue-resumed":
		return &flash{Text: "Queue resumed"}
	case "recurring-paused":
		return &flash{Text: "Recurring job paused"}
	case "recurring-resumed":
		return &flash{Text: "Recurring job resumed"}
	case "recurring-removed":
		return &flash{Text: "Recurring job removed"}
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
	h.render(w, r, code, "error", http.StatusText(code), &errorView{Code: code, Message: err.Error()})
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
