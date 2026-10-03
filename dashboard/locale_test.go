package dashboard

import (
	"encoding/json"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/memstore"
)

func catalogs(t *testing.T) map[string]map[string]string {
	t.Helper()
	out := make(map[string]map[string]string)
	entries, err := fs.ReadDir(localeFS, "locales")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := localeFS.ReadFile("locales/" + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]string
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("%s: %v", e.Name(), err)
		}
		out[strings.TrimSuffix(e.Name(), ".json")] = m
	}
	return out
}

func placeholders(s string) []string {
	var out []string
	for _, p := range compile(s) {
		if p.Arg != "" {
			out = append(out, p.Arg)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func TestCatalogs(t *testing.T) {
	t.Parallel()
	all := catalogs(t)
	en := all["en"]
	if en == nil || all["pt-BR"] == nil {
		t.Fatalf("catalogs %v", slices.Sorted(maps.Keys(all)))
	}
	for k, v := range en {
		if v == "" {
			t.Errorf("en: %s is empty", k)
		}
		if base, ok := strings.CutSuffix(k, ".one"); ok && en[base+".other"] == "" {
			t.Errorf("en: %s has no .other", k)
		}
		if base, ok := strings.CutSuffix(k, ".other"); ok && en[base+".one"] == "" {
			t.Errorf("en: %s has no .one", k)
		}
	}
	for tag, c := range all {
		for k, v := range en {
			got, ok := c[k]
			if !ok {
				t.Errorf("%s: missing %s", tag, k)
				continue
			}
			if got == "" {
				t.Errorf("%s: %s is empty", tag, k)
			}
			if a, b := placeholders(v), placeholders(got); !slices.Equal(a, b) {
				t.Errorf("%s: %s has placeholders %v, en has %v", tag, k, b, a)
			}
		}
		for k := range c {
			if _, ok := en[k]; !ok {
				t.Errorf("%s: %s is not in en", tag, k)
			}
		}
		if lc := match(tag); lc == nil || lc.Tag != tag || lc.Name != c["lang.name"] {
			t.Errorf("%s: not loaded", tag)
		}
	}
}

func TestCatalogKeysUsed(t *testing.T) {
	t.Parallel()
	en := catalogs(t)["en"]
	spaces := make(map[string]bool)
	for k := range en {
		ns, _, _ := strings.Cut(k, ".")
		spaces[ns] = true
	}
	has := func(where, key string) {
		t.Helper()
		if _, ok := en[key]; ok {
			return
		}
		if _, ok := en[key+".one"]; ok {
			if _, ok := en[key+".other"]; ok {
				return
			}
		}
		t.Errorf("%s uses %s, which is not in the catalogs", where, key)
	}

	tmplKey := regexp.MustCompile(`\b(t|tn|parts) "([^"]+)"`)
	files, err := fs.Glob(templateFS, "templates/*.html")
	if err != nil || len(files) == 0 {
		t.Fatalf("templates: %v", err)
	}
	for _, f := range files {
		b, _ := templateFS.ReadFile(f)
		for _, m := range tmplKey.FindAllStringSubmatch(string(b), -1) {
			if m[1] == "tn" {
				has(f, m[2]+".one")
				has(f, m[2]+".other")
			} else {
				has(f, m[2])
			}
		}
	}

	goKey := regexp.MustCompile(`"([a-z]+(?:\.[a-z0-9_]+)+)"`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range goKey.FindAllStringSubmatch(string(b), -1) {
			if ns, _, _ := strings.Cut(m[1], "."); spaces[ns] {
				has(name, m[1])
			}
		}
	}
	for _, s := range tabOrder {
		has("label", "state."+string(s))
		has("label", "state."+string(s)+".jobs")
		has("label", "state."+string(s)+".job")
	}
	for _, code := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusInternalServerError} {
		has("error.html", "status."+strconv.Itoa(code))
	}
	for i := 1; i <= 12; i++ {
		has("stamp", "month."+strconv.Itoa(i))
	}

	script := make(map[string]bool)
	for _, keys := range scriptKeys {
		for _, k := range keys {
			has("scriptKeys", k)
			script[k] = true
		}
	}
	js, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	used := []string{"quip.fresh", "quip.cold", "quip.failed", "quip.busy", "quip.waiting", "quip.idle", "quip.party"}
	for _, m := range regexp.MustCompile(`\b(t|plural)\('([^']+)'`).FindAllStringSubmatch(string(js), -1) {
		if m[1] == "plural" {
			used = append(used, m[2]+".one", m[2]+".other")
		} else {
			used = append(used, m[2])
		}
	}
	for _, k := range used {
		has("app.js", k)
		if !script[k] {
			t.Errorf("app.js uses %s, which no page sends", k)
		}
	}
	for k := range script {
		if !slices.Contains(used, k) {
			t.Errorf("pages send %s, which app.js does not use", k)
		}
	}
}

func TestNegotiate(t *testing.T) {
	t.Parallel()
	for header, want := range map[string]string{
		"":                                       "",
		"pt-BR":                                  "pt-BR",
		"pt":                                     "pt-BR",
		"pt-PT":                                  "pt-BR",
		"PT-br":                                  "pt-BR",
		"en-US,en;q=0.9":                         "en",
		"fr, pt;q=0.5":                           "pt-BR",
		"pt;q=0.4, en;q=0.8":                     "en",
		"en;q=0.8, pt-BR":                        "pt-BR",
		"fr-CA, fr;q=0.9, pt-BR;q=0.8, en;q=0.7": "pt-BR",
		"de, *;q=0.5":                            "",
		"pt-BR;q=0":                              "",
		"pt;q=abc, en;q=0.1":                     "en",
	} {
		got := ""
		if lc := negotiate(header); lc != nil {
			got = lc.Tag
		}
		if got != want {
			t.Errorf("negotiate(%q) = %q, want %q", header, got, want)
		}
	}
}

func TestLanguage(t *testing.T) {
	t.Parallel()
	c := kiln.NewClient(memstore.New())
	tests := []struct {
		option, cookie, accept string
		want                   string
	}{
		{"", "", "", "en"},
		{"", "", "pt-BR,pt;q=0.9,en;q=0.8", "pt-BR"},
		{"", "", "pt-PT", "pt-BR"},
		{"", "", "fr, en;q=0.5", "en"},
		{"", "", "fr", "en"},
		{"pt-BR", "", "en", "pt-BR"},
		{"pt-BR", "", "", "pt-BR"},
		{"pt-BR", "en", "pt-BR", "en"},
		{"", "pt-BR", "en", "pt-BR"},
		{"", "pt", "", "pt-BR"},
		{"", "xx", "pt", "pt-BR"},
		{"xx", "", "pt", "pt-BR"},
		{"xx", "", "", "en"},
		{"en", "xx", "pt-BR", "en"},
	}
	for _, tt := range tests {
		h := New(c, Options{Authorize: grant(ReadOnly), Language: tt.option})
		r := httptest.NewRequest(http.MethodGet, "/queues", nil)
		if tt.cookie != "" {
			r.AddCookie(&http.Cookie{Name: "kiln_lang", Value: tt.cookie})
		}
		if tt.accept != "" {
			r.Header.Set("Accept-Language", tt.accept)
		}
		if got := h.(*handler).locale(r).Tag; got != tt.want {
			t.Errorf("option %q, cookie %q, Accept-Language %q: %s, want %s", tt.option, tt.cookie, tt.accept, got, tt.want)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if want := `<html lang="` + tt.want + `">`; !strings.Contains(w.Body.String(), want) {
			t.Errorf("option %q, cookie %q, Accept-Language %q: page lacks %s", tt.option, tt.cookie, tt.accept, want)
		}
	}
}

func TestLanguagePicker(t *testing.T) {
	t.Parallel()
	f := seed(t)
	for _, prefix := range []string{"", "/kiln"} {
		h := New(f.c, Options{Authorize: grant(ReadOnly), Prefix: prefix})
		res := get(h, prefix+"/jobs/failed?queue=work&lang=pt-BR")
		if res.code != http.StatusSeeOther || res.hdr.Get("Location") != prefix+"/jobs/failed?queue=work" {
			t.Fatalf("%q: %d %q", prefix, res.code, res.hdr.Get("Location"))
		}
		path := prefix
		if path == "" {
			path = "/"
		}
		want := "kiln_lang=pt-BR; Path=" + path + "; Max-Age=31536000; HttpOnly; SameSite=Lax"
		if got := res.hdr.Get("Set-Cookie"); got != want {
			t.Errorf("%q: cookie %q, want %q", prefix, got, want)
		}

		page := do(h, http.MethodGet, res.hdr.Get("Location"), nil, "Cookie", "kiln_lang=pt-BR")
		for _, w := range []string{`<html lang="pt-BR">`, `<form class="lang" method="get"><input type="hidden" name="queue" value="work">`,
			`<option value="pt-BR" lang="pt-BR" selected>Português (Brasil)</option>`, `<option value="en" lang="en">English</option>`,
			`<noscript><button class="btn sm" type="submit">Aplicar</button></noscript>`} {
			if !strings.Contains(page.body, w) {
				t.Errorf("%q: page lacks %s", prefix, w)
			}
		}

		res = get(h, prefix+"/?lang=pt")
		if res.code != http.StatusSeeOther || res.hdr.Get("Location") != prefix+"/" || !strings.HasPrefix(res.hdr.Get("Set-Cookie"), "kiln_lang=pt-BR;") {
			t.Errorf("%q: base tag: %d %q %q", prefix, res.code, res.hdr.Get("Location"), res.hdr.Get("Set-Cookie"))
		}
		res = get(h, prefix+"/queues?lang=xx")
		if res.code != http.StatusSeeOther || res.hdr.Get("Location") != prefix+"/queues" || res.hdr.Get("Set-Cookie") != "" {
			t.Errorf("%q: unknown tag: %d %q %q", prefix, res.code, res.hdr.Get("Location"), res.hdr.Get("Set-Cookie"))
		}
		res = get(h, prefix+"/api/jobs/failed?lang=pt-BR")
		if res.code != http.StatusOK || res.hdr.Get("Set-Cookie") != "" || !strings.HasPrefix(res.hdr.Get("Content-Type"), "application/json") {
			t.Errorf("%q: api: %d %q", prefix, res.code, res.hdr.Get("Set-Cookie"))
		}
	}
}

func TestPortuguesePages(t *testing.T) {
	t.Parallel()
	f := seed(t)
	h := New(f.c, Options{Authorize: grant(ReadWrite), Language: "pt-BR", Actor: func(*http.Request) string { return "ana" }})
	id := func(n int64) string { return strconv.FormatInt(n, 10) }
	tests := []struct {
		path      string
		want, not []string
	}{
		{"/", []string{`<html lang="pt-BR">`, "<title>Visão geral · kiln</title>", ">Olá, ana<", "Jobs por estado", "Retidos",
			`data-plural-one="job com falha precisa de atenção"`, ">job com falha precisa de atenção<", ">Falhou<", ">Concluído<",
			"Vazão", "Nenhum servidor em execução", ">Ao vivo<", ">Visão geral<", ">Retentativas<", ">Lotes<"},
			[]string{"Overview", "Throughput", "Jobs by state", "Held back", "Live<"}},
		{"/jobs/failed", []string{"<title>Jobs com falha · kiln</title>", ">Reenfileirar todos<",
			`data-confirm="Excluir todo job com falha que corresponda aos filtros atuais?"`, `data-confirm="Excluir os jobs selecionados?"`,
			">Finalizado<", ">Na fila<"}, []string{"Requeue", "Finished"}},
		{"/jobs/processing", []string{">Cancelar todos<", ">Iniciado<"}, nil},
		{"/jobs/failed?queue=nope", []string{"Nenhum job com falha corresponde a esses filtros."}, nil},
		{"/jobs/scheduled?tag=x", []string{"Nenhum job agendado corresponde a esses filtros."}, nil},
		{"/retries", []string{"<title>Retentativas · kiln</title>", "Próxima tentativa"}, nil},
		{"/jobs/" + id(f.failed), []string{"<title>Job " + id(f.failed) + " · kiln</title>", ">Jobs com falha</a>",
			`Job <span class="mono">` + id(f.failed) + `</span> na fila <a href="/jobs/failed?queue=work">work</a> · tentativa 1 de 1`,
			"Histórico", ">Criado<", `data-confirm="Excluir o job ` + id(f.failed) + `?"`, "<dt>Tentativas</dt><dd class=\"num\">1 de 1</dd>"},
			[]string{"History", "Created"}},
		{"/jobs/" + id(f.processing), []string{`data-confirm="Cancelar o job ` + id(f.processing) + `?"`}, nil},
		{"/jobs/" + id(f.awaiting), []string{"<dt>Aguardando</dt><dd>1 pendente</dd>", "<dt>Pais</dt>"}, nil},
		{"/queues", []string{"6 filas", ">Pausada<", ">Ativa<", `data-confirm="Pausar a fila mailers? Os workers param de pegar os jobs dela."`}, nil},
		{"/recurring", []string{"1 agendamento", ">Disparar<", `data-confirm="Remover o job recorrente nightly?"`}, nil},
		{"/servers", []string{"1 registrado", "Todos os tipos"}, nil},
		{"/batches", []string{"<title>Lotes · kiln</title>", ">Em andamento<", ">Descrição<"}, nil},
		{"/batches/" + id(f.batch), []string{"<title>Lote " + id(f.batch) + " · kiln</title>", "0 de 2 jobs concluídos · 0%"}, nil},
		{"/limits", []string{"<title>Limites · kiln</title>", ">Chave<"}, nil},
		{"/jobs/" + id(f.failed) + "?done=requeued&n=2", []string{"2 jobs reenfileirados"}, nil},
		{"/recurring?done=triggered&n=7", []string{"Job 7 enfileirado", ">Ver job<"}, nil},
	}
	for _, tt := range tests {
		res := get(h, tt.path)
		if res.code != http.StatusOK {
			t.Fatalf("%s: status %d", tt.path, res.code)
		}
		for _, w := range tt.want {
			if !strings.Contains(res.body, w) {
				t.Errorf("%s: missing %q", tt.path, w)
			}
		}
		for _, w := range tt.not {
			if strings.Contains(res.body, w) {
				t.Errorf("%s: has %q", tt.path, w)
			}
		}
	}

	res := get(h, "/nope")
	if res.code != http.StatusNotFound {
		t.Errorf("404: %d", res.code)
	}
	if res := get(h, "/jobs/999999"); res.code != http.StatusNotFound || !strings.Contains(res.body, "<h1>Não encontrado</h1>") ||
		!strings.Contains(res.body, "Voltar para a visão geral") {
		t.Errorf("error page: %d", res.code)
	}

	raw := catalogs(t)["pt-BR"]
	block := regexp.MustCompile(`<script type="application/json" data-text>(.*?)</script>`)
	for view, path := range map[string]string{"overview": "/", "jobs": "/jobs/failed", "job": "/jobs/" + id(f.failed)} {
		m := block.FindStringSubmatch(get(h, path).body)
		if m == nil {
			t.Fatalf("%s: no text block", path)
		}
		var got map[string]string
		if err := json.Unmarshal([]byte(m[1]), &got); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if !slices.Equal(slices.Sorted(maps.Keys(got)), slices.Sorted(slices.Values(scriptKeys[view]))) {
			t.Errorf("%s: keys %v", path, slices.Sorted(maps.Keys(got)))
		}
		for k, v := range got {
			if v != raw[k] {
				t.Errorf("%s: %s = %q, want %q", path, k, v, raw[k])
			}
		}
	}
	if strings.Contains(get(h, "/queues").body, "data-text") {
		t.Error("queues page sends strings no script uses")
	}
}

func TestPortugueseFormat(t *testing.T) {
	t.Parallel()
	pt := match("pt-BR")
	now := time.Now()
	at := time.Date(2026, time.October, 3, 7, 4, 53, 0, time.UTC)
	for _, tt := range []struct{ got, want string }{
		{pt.num(1234567), "1.234.567"},
		{pt.num(-4200), "-4.200"},
		{pt.num(999), "999"},
		{pt.short(167412), "167,4k"},
		{pt.short(1234567), "1,2M"},
		{pt.short(99999), "99.999"},
		{pt.ago(now), "agora"},
		{pt.ago(now.Add(-3*time.Minute - 10*time.Second)), "há 3min"},
		{pt.ago(now.Add(-72 * time.Hour)), "há 3d"},
		{pt.ago(now.Add(12*time.Second + 500*time.Millisecond)), "em 12s"},
		{pt.dur(2500 * time.Millisecond), "2,5s"},
		{pt.dur(3*time.Minute + 20*time.Second), "3min 20s"},
		{pt.dur(2*time.Hour + 5*time.Minute), "2h 5min"},
		{pt.abs(at), "03/10/2026 07:04:53 UTC"},
		{pt.stamp(at), "3 out 07:04 UTC"},
		{english.abs(at), "2026-10-03 07:04:53 UTC"},
		{english.stamp(at), "Oct 3 07:04 UTC"},
		{pt.label(driver.Succeeded), "Concluído"},
		{pt.label(driver.Failed, "jobs"), "Jobs com falha"},
		{pt.label(driver.Failed, "job"), "com falha"},
		{pt.plural("flash.deleted", 1), "1 job excluído"},
		{pt.plural("flash.deleted", 1500), "1.500 jobs excluídos"},
		{pt.text("job.sub", "id", "7"), "Job 7 na fila {queue}"},
		{pt.text("nope.nope"), "nope.nope"},
	} {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}
