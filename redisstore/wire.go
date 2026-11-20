package redisstore

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	maxError = 2 << 10
	maxTrace = 8 << 10
	maxTitle = 200
)

type wireParent struct {
	ID int64 `json:"id,omitempty"`
	Ix int   `json:"ix"`
	On uint8 `json:"on"`
}

type wireParams struct {
	K      string       `json:"k"`
	Q      string       `json:"q"`
	Args   string       `json:"args"`
	Meta   string       `json:"meta,omitempty"`
	Tags   string       `json:"tags,omitempty"`
	Title  string       `json:"title,omitempty"`
	Pri    int16        `json:"pri,omitempty"`
	Max    int          `json:"max"`
	Tmo    string       `json:"tmo"`
	HR     bool         `json:"hr,omitempty"`
	Run    int64        `json:"run,omitempty"`
	Delay  int64        `json:"delay,omitempty"`
	UK     string       `json:"uk,omitempty"`
	UFor   int64        `json:"ufor,omitempty"`
	URep   bool         `json:"urep,omitempty"`
	UDeb   int64        `json:"udeb,omitempty"`
	LK     string       `json:"lk,omitempty"`
	LMax   int          `json:"lmax,omitempty"`
	LRate  int          `json:"lrate,omitempty"`
	LPer   int64        `json:"lper,omitempty"`
	LBurst int          `json:"lburst,omitempty"`
	B      int64        `json:"b,omitempty"`
	AB     int64        `json:"ab,omitempty"`
	Par    []wireParent `json:"par,omitempty"`
	Rec    string       `json:"rec,omitempty"`
}

func params(jobs []driver.InsertParams) []wireParams {
	out := make([]wireParams, len(jobs))
	for i := range jobs {
		p := &jobs[i]
		w := wireParams{
			K: p.Kind, Q: p.Queue, Args: string(p.Args),
			Meta: object(p.Meta), Tags: list(p.Tags), Title: clip(p.Title, maxTitle),
			Pri: p.Priority, Max: p.MaxAttempts, Tmo: strconv.FormatInt(int64(p.Timeout), 10),
			HR: !p.RunAt.IsZero(), Delay: micros(p.Delay),
			UFor: micros(p.UniqueFor), URep: p.UniqueReplace, UDeb: micros(p.UniqueDebounce),
			LK: p.LimitKey, LMax: p.LimitMax, LRate: p.LimitRate, LPer: micros(p.LimitPer), LBurst: p.LimitBurst,
			B: p.BatchID, AB: p.AfterBatch, Rec: p.RecurringID,
		}
		if w.HR {
			w.Run = p.RunAt.UnixMicro()
		}
		if len(p.UniqueKey) > 0 {
			w.UK = base64.RawStdEncoding.EncodeToString(p.UniqueKey)
		}
		for _, par := range p.Parents {
			w.Par = append(w.Par, wireParent{ID: par.ID, Ix: par.Index, On: uint8(par.On)})
		}
		out[i] = w
	}
	return out
}

func object(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func list(s []string) string {
	if len(s) == 0 {
		return ""
	}
	b, _ := json.Marshal(s)
	return string(b)
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

type wireInserted struct {
	ID  int64  `json:"id,string"`
	St  string `json:"st"`
	Dup bool   `json:"dup"`
	Rep bool   `json:"rep"`
}

func inserted(ws []wireInserted) []driver.Inserted {
	out := make([]driver.Inserted, len(ws))
	for i, w := range ws {
		out[i] = driver.Inserted{ID: w.ID, State: driver.State(w.St), Duplicate: w.Dup, Replaced: w.Rep}
	}
	return out
}

type wireJob struct {
	ID    int64    `json:"id,string"`
	Claim int32    `json:"claim"`
	K     string   `json:"k"`
	Q     string   `json:"q"`
	Args  string   `json:"args"`
	Meta  string   `json:"meta"`
	Tags  string   `json:"tags"`
	Title string   `json:"title"`
	Pri   int16    `json:"pri"`
	Att   int      `json:"att"`
	Max   int      `json:"max"`
	Tmo   int64    `json:"tmo,string"`
	Run   int64    `json:"run,string"`
	Cr    int64    `json:"cr,string"`
	At    int64    `json:"at,string"`
	B     int64    `json:"b,string"`
	Rec   string   `json:"rec"`
	Par   []string `json:"par"`
	Lim   string   `json:"lim"`

	St     string   `json:"st"`
	Fin    int64    `json:"fin,string"`
	Srv    string   `json:"srv"`
	Cancel bool     `json:"cancel"`
	AB     int64    `json:"ab,string"`
	Pend   int      `json:"pend"`
	Prog   int      `json:"prog"`
	Hist   []string `json:"hist"`
	Out    string   `json:"out"`
	Ch     []string `json:"ch"`
}

func (w *wireJob) job() driver.Job {
	j := driver.Job{
		Ref:         driver.Ref{ID: w.ID, Claim: w.Claim},
		Kind:        w.K,
		Queue:       w.Q,
		Args:        []byte(w.Args),
		Title:       w.Title,
		Priority:    w.Pri,
		Attempt:     w.Att,
		MaxAttempts: w.Max,
		Timeout:     time.Duration(w.Tmo),
		RunAt:       when(w.Run),
		CreatedAt:   when(w.Cr),
		AttemptedAt: when(w.At),
		BatchID:     w.B,
		RecurringID: w.Rec,
		Parents:     ints(w.Par),
		LimitKey:    w.Lim,
	}
	if w.Meta != "" {
		json.Unmarshal([]byte(w.Meta), &j.Meta)
	}
	if w.Tags != "" {
		json.Unmarshal([]byte(w.Tags), &j.Tags)
	}
	return j
}

type wireEntry struct {
	At     int64  `json:"at,string"`
	St     string `json:"st"`
	Att    int    `json:"att"`
	Reason string `json:"reason"`
	Err    string `json:"err"`
	Trace  string `json:"trace"`
	Srv    string `json:"srv"`
}

func (w *wireJob) record() driver.Record {
	r := driver.Record{
		Job:             w.job(),
		State:           driver.State(w.St),
		FinalizedAt:     when(w.Fin),
		Server:          w.Srv,
		CancelRequested: w.Cancel,
		AfterBatch:      w.AB,
		PendingDeps:     w.Pend,
		Progress:        w.Prog,
		Children:        sortedInts(w.Ch),
	}
	if w.Out != "" {
		r.Output = []byte(w.Out)
	}
	for _, h := range w.Hist {
		var e wireEntry
		if json.Unmarshal([]byte(h), &e) != nil {
			continue
		}
		r.History = append(r.History, driver.Entry{
			At: when(e.At), State: driver.State(e.St), Attempt: e.Att,
			Reason: e.Reason, Error: e.Err, Trace: e.Trace, Server: e.Srv,
		})
	}
	return r
}
