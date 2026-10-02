package redisbus

import (
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
)

func TestCodec(t *testing.T) {
	t.Parallel()
	mixed := []driver.Event{
		{Kind: driver.JobsReady, Queue: "default"},
		{Kind: driver.CancelRequested, ID: 1},
		{Kind: driver.QueueChanged, Queue: "mail"},
		{Kind: driver.Resync},
		{Kind: driver.CancelRequested, ID: math.MaxInt64},
		{Kind: driver.JobsReady, Queue: strings.Repeat("q", 300)},
	}
	tests := []struct {
		name string
		in   []driver.Event
		want []driver.Event
		wire string
	}{
		{name: "resync", in: []driver.Event{{Kind: driver.Resync}}, wire: "\x01\x00"},
		{name: "jobs ready", in: []driver.Event{{Kind: driver.JobsReady, Queue: "mail"}}, wire: "\x02\x04mail"},
		{name: "cancel", in: []driver.Event{{Kind: driver.CancelRequested, ID: 300}}, wire: "\x03\x02\xac\x02"},
		{name: "queue changed", in: []driver.Event{{Kind: driver.QueueChanged, Queue: "q"}}, wire: "\x04\x01q"},
		{name: "empty queue", in: []driver.Event{{Kind: driver.JobsReady}}, wire: "\x02\x00"},
		{name: "utf-8 queue", in: []driver.Event{{Kind: driver.QueueChanged, Queue: "fila-ação"}}},
		{name: "negative id", in: []driver.Event{{Kind: driver.CancelRequested, ID: -7}}},
		{name: "batch", in: mixed},
		{
			name: "fields the kind does not carry",
			in:   []driver.Event{{Kind: driver.JobsReady, Queue: "a", ID: 9}, {Kind: driver.CancelRequested, Queue: "b", ID: 9}, {Kind: driver.Resync, Queue: "c", ID: 9}},
			want: []driver.Event{{Kind: driver.JobsReady, Queue: "a"}, {Kind: driver.CancelRequested, ID: 9}, {Kind: driver.Resync}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := appendEvents(nil, tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if tt.wire != "" && string(p) != tt.wire {
				t.Errorf("wire %q, want %q", p, tt.wire)
			}
			want := tt.want
			if want == nil {
				want = tt.in
			}
			if got := slices.Collect(decode(string(p))); !slices.Equal(got, want) {
				t.Errorf("decoded %v, want %v", got, want)
			}
		})
	}
}

func TestEncodeInvalidKind(t *testing.T) {
	t.Parallel()
	for _, k := range []driver.EventKind{0, driver.QueueChanged + 1, 255} {
		_, err := appendEvents(nil, []driver.Event{{Kind: driver.JobsReady, Queue: "a"}, {Kind: k}})
		if !errors.Is(err, driver.ErrInvalid) {
			t.Errorf("kind %d: err %v, want ErrInvalid", k, err)
		}
	}
}

func TestDecodeMalformed(t *testing.T) {
	t.Parallel()
	ready := driver.Event{Kind: driver.JobsReady, Queue: "a"}
	tests := []struct {
		name string
		in   string
		want []driver.Event
	}{
		{name: "empty", in: ""},
		{name: "kind only", in: "\x02"},
		{name: "length past the end", in: "\x02\x01a\x02\x05ab", want: []driver.Event{ready}},
		{name: "truncated length", in: "\x02\x01a\x02\x80", want: []driver.Event{ready}},
		{name: "overlong length", in: "\x02" + strings.Repeat("\xff", 10) + "\x01"},
		{name: "unknown kind skipped", in: "\x09\x03xyz\x02\x01a", want: []driver.Event{ready}},
		{name: "zero kind skipped", in: "\x00\x00\x02\x01a", want: []driver.Event{ready}},
		{name: "empty cancel skipped", in: "\x03\x00\x02\x01a", want: []driver.Event{ready}},
		{name: "cancel with trailing bytes skipped", in: "\x03\x02\x01\x01\x02\x01a", want: []driver.Event{ready}},
		{name: "cancel with a cut varint skipped", in: "\x03\x01\x80\x02\x01a", want: []driver.Event{ready}},
		{name: "resync payload ignored", in: "\x01\x02zz\x02\x01a", want: []driver.Event{{Kind: driver.Resync}, ready}},
		{name: "valid prefix kept", in: "\x02\x01a\xff", want: []driver.Event{ready}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := slices.Collect(decode(tt.in)); !slices.Equal(got, tt.want) {
				t.Errorf("decoded %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeStops(t *testing.T) {
	t.Parallel()
	p, err := appendEvents(nil, []driver.Event{{Kind: driver.Resync}, {Kind: driver.Resync}, {Kind: driver.Resync}})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range decode(string(p)) {
		n++
		break
	}
	if n != 1 {
		t.Fatalf("yielded %d events after break", n)
	}
}

func TestCodecAllocs(t *testing.T) {
	events := []driver.Event{
		{Kind: driver.JobsReady, Queue: "default"},
		{Kind: driver.CancelRequested, ID: 123456789},
		{Kind: driver.QueueChanged, Queue: "mail"},
		{Kind: driver.Resync},
	}
	buf := make([]byte, 0, 64)
	if n := testing.AllocsPerRun(100, func() {
		buf, _ = appendEvents(buf[:0], events)
	}); n != 0 {
		t.Errorf("encoding allocates %v times", n)
	}
	p := string(buf)
	seen := 0
	if n := testing.AllocsPerRun(100, func() {
		for e := range decode(p) {
			seen += len(e.Queue)
		}
	}); n != 0 {
		t.Errorf("decoding allocates %v times", n)
	}
	if seen == 0 {
		t.Fatal("nothing decoded")
	}
}

func FuzzDecode(f *testing.F) {
	seed, err := appendEvents(nil, []driver.Event{
		{Kind: driver.Resync},
		{Kind: driver.JobsReady, Queue: "default"},
		{Kind: driver.CancelRequested, ID: 1 << 40},
		{Kind: driver.QueueChanged, Queue: "mail"},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(string(seed))
	f.Add("\x03\x0a\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01")
	f.Add("\x02\xff\xff\xff\xff\x0f")
	f.Fuzz(func(t *testing.T, p string) {
		got := slices.Collect(decode(p))
		q, err := appendEvents(nil, got)
		if err != nil {
			t.Fatalf("decoded an event that does not encode: %v", err)
		}
		if again := slices.Collect(decode(string(q))); !slices.Equal(again, got) {
			t.Fatalf("round trip changed %v into %v", got, again)
		}
	})
}

func BenchmarkEncode(b *testing.B) {
	events := []driver.Event{
		{Kind: driver.JobsReady, Queue: "default"},
		{Kind: driver.CancelRequested, ID: 123456789},
		{Kind: driver.QueueChanged, Queue: "mail"},
	}
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = appendEvents(buf[:0], events)
	}
}
