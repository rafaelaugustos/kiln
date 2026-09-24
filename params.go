package kiln

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/rafaelaugustos/kiln/driver"
)

const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 10

	maxArgs     = 1 << 20
	maxTags     = 32
	maxTagLen   = 64
	maxMetaKeys = 64
	maxMetaLen  = 16 << 10

	metaDeadline   = "kiln.deadline"
	metaOccurrence = "kiln.occurrence"
)

type insertDefaults interface {
	InsertOptions() []InsertOption
}

type builder struct {
	p        driver.InsertParams
	unique   *Unique
	limit    *Limit
	deadline time.Time
	ownMeta  bool
	tz       string
	misfire  Misfire
	overlap  bool
	err      error
}

func newBuilder(args Args) (*builder, error) {
	if args == nil {
		return nil, fmt.Errorf("%w: nil args", ErrInvalid)
	}
	kind := args.Kind()
	if !validKind(kind) {
		return nil, fmt.Errorf("%w: kind %q", ErrInvalid, kind)
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("kiln: encode %s: %w", kind, err)
	}
	if len(payload) > maxArgs {
		return nil, fmt.Errorf("%w: %s args are %d bytes", ErrTooLarge, kind, len(payload))
	}
	b := &builder{
		p: driver.InsertParams{
			Kind:        kind,
			Queue:       DefaultQueue,
			Args:        payload,
			MaxAttempts: DefaultMaxAttempts,
		},
		overlap: true,
	}
	if d, ok := args.(insertDefaults); ok {
		for _, o := range d.InsertOptions() {
			b.insert(o)
		}
	}
	return b, nil
}

func buildParams(args Args, opts []InsertOption) (driver.InsertParams, error) {
	b, err := newBuilder(args)
	if err != nil {
		return driver.InsertParams{}, err
	}
	for _, o := range opts {
		b.insert(o)
	}
	return b.finish()
}

func buildRecurring(args Args, opts []RecurringOption) (driver.Recurring, *time.Location, error) {
	b, err := newBuilder(args)
	if err != nil {
		return driver.Recurring{}, nil, err
	}
	for _, o := range opts {
		b.recurring(o)
	}
	if b.tz == "Local" {
		return driver.Recurring{}, nil, fmt.Errorf("%w: time zone Local is ambiguous across servers", ErrInvalid)
	}
	loc, err := time.LoadLocation(b.tz)
	if err != nil {
		return driver.Recurring{}, nil, fmt.Errorf("%w: time zone %q: %v", ErrInvalid, b.tz, err)
	}
	p, err := b.finish()
	if err != nil {
		return driver.Recurring{}, nil, err
	}
	if len(p.Parents) > 0 || p.BatchID != 0 || p.AfterBatch != 0 {
		return driver.Recurring{}, nil, fmt.Errorf("%w: recurring jobs cannot depend on jobs or batches", ErrInvalid)
	}
	p.RunAt, p.Delay = time.Time{}, 0
	return driver.Recurring{Location: loc.String(), Template: p, Misfire: driver.Misfire(b.misfire), Overlap: b.overlap}, loc, nil
}

func (b *builder) insert(o InsertOption) {
	p := &b.p
	switch o := o.(type) {
	case Queue:
		p.Queue = string(o)
	case Priority:
		p.Priority = int16(o)
	case Delay:
		p.Delay, p.RunAt = time.Duration(o), time.Time{}
	case At:
		p.RunAt, p.Delay = time.Time(o), 0
	case MaxAttempts:
		p.MaxAttempts = int(o)
	case Timeout:
		p.Timeout = time.Duration(o)
	case Tags:
		for _, t := range o {
			if !slices.Contains(p.Tags, t) {
				p.Tags = append(p.Tags, t)
			}
		}
	case Meta:
		for k, v := range o {
			b.setMeta(k, v)
		}
	case Unique:
		b.unique = &o
	case Limit:
		b.limit = &o
	case After:
		b.after(o, driver.OnSucceeded)
	case AfterFinished:
		b.after(o, driver.OnFinished)
	case Needs:
		for _, i := range o {
			p.Parents = append(p.Parents, driver.Parent{Index: i, On: driver.OnSucceeded})
		}
	case AfterBatch:
		p.AfterBatch = int64(o)
	case InBatch:
		p.BatchID = int64(o)
	case Deadline:
		b.deadline = time.Time(o)
	case nil:
	default:
		panic(fmt.Sprintf("kiln: unknown insert option %T", o))
	}
}

func (b *builder) recurring(o RecurringOption) {
	switch o := o.(type) {
	case TZ:
		b.tz = string(o)
	case Misfire:
		b.misfire = o
	case Overlap:
		b.overlap = bool(o)
	case InsertOption:
		b.insert(o)
	case nil:
	default:
		panic(fmt.Sprintf("kiln: unknown recurring option %T", o))
	}
}

func (b *builder) after(ids []int64, on driver.Mask) {
	for _, id := range ids {
		if id <= 0 && b.err == nil {
			b.err = fmt.Errorf("%w: parent id %d", ErrInvalid, id)
		}
		b.p.Parents = append(b.p.Parents, driver.Parent{ID: id, On: on})
	}
}

func (b *builder) setMeta(k, v string) {
	if !b.ownMeta {
		m := make(map[string]string, len(b.p.Meta)+1)
		maps.Copy(m, b.p.Meta)
		b.p.Meta, b.ownMeta = m, true
	}
	b.p.Meta[k] = v
}

func (b *builder) finish() (driver.InsertParams, error) {
	p := b.p
	if b.err != nil {
		return p, b.err
	}
	if !validQueue(p.Queue) {
		return p, fmt.Errorf("%w: queue %q", ErrInvalid, p.Queue)
	}
	if p.MaxAttempts < 1 {
		return p, fmt.Errorf("%w: max attempts %d", ErrInvalid, p.MaxAttempts)
	}
	if p.Delay < 0 {
		p.Delay = 0
	}
	if len(p.Tags) > maxTags {
		return p, fmt.Errorf("%w: %d tags", ErrTooLarge, len(p.Tags))
	}
	for _, t := range p.Tags {
		if t == "" || len(t) > maxTagLen {
			return p, fmt.Errorf("%w: tag %q", ErrInvalid, t)
		}
	}
	if !b.deadline.IsZero() {
		b.setMeta(metaDeadline, b.deadline.UTC().Format(time.RFC3339Nano))
		p.Meta = b.p.Meta
	}
	if err := checkMeta(p.Meta); err != nil {
		return p, err
	}
	if u := b.unique; u != nil {
		key := u.Key
		if key == "" {
			key = string(p.Args)
		}
		p.UniqueKey = uniqueKey(p.Kind, key)
		p.UniqueFor = max(u.For, 0)
	}
	if l := b.limit; l != nil {
		p.LimitKey, p.LimitMax = l.Key, l.Max
		if p.LimitKey == "" {
			p.LimitKey = p.Kind
		}
		if p.LimitMax < 1 {
			p.LimitMax = 1
		}
		if len(p.LimitKey) > 200 {
			return p, fmt.Errorf("%w: limit key is %d bytes", ErrTooLarge, len(p.LimitKey))
		}
	}
	return p, nil
}

func checkMeta(m map[string]string) error {
	if len(m) > maxMetaKeys {
		return fmt.Errorf("%w: %d meta keys", ErrTooLarge, len(m))
	}
	n := 0
	for k, v := range m {
		if k == "" {
			return fmt.Errorf("%w: empty meta key", ErrInvalid)
		}
		n += len(k) + len(v)
	}
	if n > maxMetaLen {
		return fmt.Errorf("%w: meta is %d bytes", ErrTooLarge, n)
	}
	return nil
}

func uniqueKey(kind, key string) []byte {
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return h.Sum(nil)[:16]
}

func validKind(s string) bool {
	if len(s) == 0 || len(s) > 128 || !isLetter(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !isLetter(c) && !isDigit(c) && c != '_' && c != '.' && c != ':' && c != '-' {
			return false
		}
	}
	return true
}

func validQueue(s string) bool {
	if len(s) == 0 || len(s) > 64 || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('a' <= c && c <= 'z') && !isDigit(c) && c != '_' && c != '.' && c != ':' && c != '-' {
			return false
		}
	}
	return true
}

func isLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }
func isDigit(c byte) bool  { return '0' <= c && c <= '9' }
