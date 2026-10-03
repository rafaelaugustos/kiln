package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

//go:embed locales
var localeFS embed.FS

const langCookie = "kiln_lang"

var scriptKeys = map[string][]string{
	"overview": {"live", "live.lost", "chart.succeeded", "chart.failed", "chart.summary", "chart.error",
		"count.thousand", "count.million", "hello.name", "hello.morning", "hello.afternoon", "hello.evening",
		"hello.late", "hello.late_name", "quip.fresh", "quip.cold", "quip.failed", "quip.busy", "quip.waiting",
		"quip.idle", "quip.party"},
	"jobs":    {"selected.one", "selected.other"},
	"retries": {"selected.one", "selected.other"},
	"job":     {"console.attempt", "progress", "date.full"},
}

type piece struct {
	Text string
	Arg  string
}

type message []piece

type locale struct {
	Tag  string
	Name string

	msgs    map[string]message
	group   string
	point   string
	months  [12]string
	scripts map[string]template.JS
}

var (
	locales = loadLocales()
	english = match("en")
)

func loadLocales() []*locale {
	entries, _ := fs.ReadDir(localeFS, "locales")
	out := make([]*locale, 0, len(entries))
	for _, e := range entries {
		b, _ := localeFS.ReadFile("locales/" + e.Name())
		var raw map[string]string
		if err := json.Unmarshal(b, &raw); err != nil {
			panic("dashboard: locales/" + e.Name() + ": " + err.Error())
		}
		lc := &locale{
			Tag:     strings.TrimSuffix(e.Name(), ".json"),
			Name:    raw["lang.name"],
			msgs:    make(map[string]message, len(raw)),
			group:   raw["num.group"],
			point:   raw["num.decimal"],
			scripts: make(map[string]template.JS, len(scriptKeys)),
		}
		for k, v := range raw {
			lc.msgs[k] = compile(v)
		}
		for i := range lc.months {
			lc.months[i] = raw["month."+strconv.Itoa(i+1)]
		}
		for view, keys := range scriptKeys {
			sub := make(map[string]string, len(keys))
			for _, k := range keys {
				sub[k] = raw[k]
			}
			js, _ := json.Marshal(sub)
			lc.scripts[view] = template.JS(js)
		}
		out = append(out, lc)
	}
	return out
}

func compile(s string) message {
	var m message
	for {
		open := strings.IndexByte(s, '{')
		if open < 0 {
			break
		}
		end := strings.IndexByte(s[open:], '}')
		if end < 0 {
			break
		}
		m = append(m, piece{Text: s[:open], Arg: s[open+1 : open+end]})
		s = s[open+end+1:]
	}
	if s != "" || m == nil {
		m = append(m, piece{Text: s})
	}
	return m
}

func (lc *locale) text(key string, args ...string) string {
	m, ok := lc.msgs[key]
	if !ok {
		return key
	}
	if len(m) == 1 && m[0].Arg == "" {
		return m[0].Text
	}
	var b strings.Builder
	for _, p := range m {
		b.WriteString(p.Text)
		if p.Arg != "" {
			b.WriteString(lookup(args, p.Arg))
		}
	}
	return b.String()
}

func (lc *locale) plural(key string, n int64, args ...string) string {
	form := ".other"
	if n == 1 {
		form = ".one"
	}
	return lc.text(key+form, append([]string{"n", lc.num(n)}, args...)...)
}

func lookup(args []string, name string) string {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == name {
			return args[i+1]
		}
	}
	return "{" + name + "}"
}

func strs(args []any) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if s, ok := a.(string); ok {
			out[i] = s
		} else {
			out[i] = fmt.Sprint(a)
		}
	}
	return out
}

func match(tag string) *locale {
	for _, lc := range locales {
		if strings.EqualFold(lc.Tag, tag) {
			return lc
		}
	}
	base, _, _ := strings.Cut(tag, "-")
	for _, lc := range locales {
		if b, _, _ := strings.Cut(lc.Tag, "-"); strings.EqualFold(b, base) {
			return lc
		}
	}
	return nil
}

func negotiate(header string) *locale {
	var best *locale
	top := 0.0
	for item := range strings.SplitSeq(header, ",") {
		tag, params, _ := strings.Cut(item, ";")
		q := 1.0
		if v, ok := strings.CutPrefix(strings.TrimSpace(params), "q="); ok {
			var err error
			if q, err = strconv.ParseFloat(v, 64); err != nil {
				continue
			}
		}
		if q <= top {
			continue
		}
		if lc := match(strings.TrimSpace(tag)); lc != nil {
			best, top = lc, q
		}
	}
	return best
}

func (h *handler) locale(r *http.Request) *locale {
	if c, err := r.Cookie(langCookie); err == nil {
		if lc := match(c.Value); lc != nil {
			return lc
		}
	}
	if h.lang != nil {
		return h.lang
	}
	if lc := negotiate(r.Header.Get("Accept-Language")); lc != nil {
		return lc
	}
	return english
}

func (h *handler) pick(w http.ResponseWriter, r *http.Request, p string, q url.Values) {
	if lc := match(q.Get("lang")); lc != nil {
		http.SetCookie(w, &http.Cookie{
			Name:     langCookie,
			Value:    lc.Tag,
			Path:     h.link(),
			MaxAge:   365 * 24 * 60 * 60,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
	}
	q.Del("lang")
	h.redirect(w, r, p, q)
}
