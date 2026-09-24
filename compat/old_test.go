package compat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

var published = []string{
	"github.com/rafaelaugustos/kiln",
	"github.com/rafaelaugustos/kiln/pgstore",
	"github.com/rafaelaugustos/kiln/mysqlstore",
}

type binary struct {
	path string
	mods map[string]string
}

func build(t *testing.T) binary {
	t.Helper()
	gocmd, err := exec.LookPath("go")
	if err != nil {
		t.Skip("the go command is not in PATH, cannot build the v0.1.0 binary")
	}
	env := append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
	dl := exec.Command(gocmd, append([]string{"mod", "download", "-json"}, published...)...)
	dl.Dir, dl.Env = "v010", env
	out, err := dl.Output()
	mods := make(map[string]string)
	var failed []string
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var m struct{ Path, Version, Dir, Error string }
		derr := dec.Decode(&m)
		if derr == io.EOF {
			break
		}
		if derr != nil {
			t.Fatalf("go mod download printed %q: %v", out, derr)
		}
		if m.Error != "" {
			failed = append(failed, m.Path+"@"+m.Version+": "+m.Error)
		}
		mods[m.Path] = m.Dir
	}
	if err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok && len(ee.Stderr) > 0 {
			failed = append(failed, string(bytes.TrimSpace(ee.Stderr)))
		}
		msg := strings.Join(failed, "\n")
		if strings.Contains(msg, "SECURITY ERROR") {
			t.Fatalf("the published v0.1.0 modules do not match v010/go.sum:\n%s", msg)
		}
		t.Skipf("cannot download kiln v0.1.0, is the module proxy reachable? %v\n%s", err, msg)
	}
	path := filepath.Join(t.TempDir(), "v010")
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	cmd := exec.Command(gocmd, "build", "-o", path, ".")
	cmd.Dir, cmd.Env = "v010", env
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the v0.1.0 binary: %v\n%s", err, out)
	}
	return binary{path: path, mods: mods}
}

type proc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan []byte
	stderr bytes.Buffer
	exited chan struct{}
	err    error
}

func start(t *testing.T, bin string, args ...string) *proc {
	t.Helper()
	p := &proc{t: t, cmd: exec.Command(bin, args...), lines: make(chan []byte, 16), exited: make(chan struct{})}
	p.cmd.Stderr = &p.stderr
	stdin, err := p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start v0.1.0: %v", err)
	}
	p.stdin = stdin
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(nil, 64<<20)
		for sc.Scan() {
			p.lines <- bytes.Clone(sc.Bytes())
		}
		close(p.lines)
		p.err = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		p.cmd.Process.Kill()
		for range p.lines {
		}
		<-p.exited
	})
	return p
}

func (p *proc) recv(what string) message {
	p.t.Helper()
	select {
	case b, ok := <-p.lines:
		if !ok {
			<-p.exited
			p.t.Fatalf("v0.1.0 exited (%v) while the test waited for %s\n%s", p.err, what, p.stderr.Bytes())
		}
		var m message
		if err := json.Unmarshal(b, &m); err != nil {
			p.t.Fatalf("v0.1.0 printed %q: %v", b, err)
		}
		return m
	case <-time.After(time.Minute):
		p.t.Fatalf("v0.1.0 printed nothing for a minute while the test waited for %s", what)
	}
	return message{}
}

func (p *proc) result() *result {
	p.t.Helper()
	m := p.recv("a result")
	switch {
	case m.Result == nil:
		p.t.Fatalf("v0.1.0 printed a summary instead of a result: %+v", m.Summary)
	case m.Result.Err != "":
		p.t.Fatalf("v0.1.0 client: %s", m.Result.Err)
	}
	return m.Result
}

func (p *proc) send(pl plan) *result {
	p.t.Helper()
	b, err := json.Marshal(pl)
	if err != nil {
		p.t.Fatal(err)
	}
	if _, err := p.stdin.Write(append(b, '\n')); err != nil {
		p.t.Fatalf("send a plan to v0.1.0: %v", err)
	}
	return p.result()
}

func (p *proc) stop() (*summary, error) {
	p.t.Helper()
	p.stdin.Close()
	m := p.recv("its summary")
	if m.Summary == nil {
		p.t.Fatalf("v0.1.0 printed a result instead of its summary: %+v", m.Result)
	}
	select {
	case <-p.exited:
	case <-time.After(30 * time.Second):
		p.t.Fatal("v0.1.0 printed its summary but did not exit")
	}
	return m.Summary, p.err
}

func once(bin string, args ...string) (*summary, error) {
	out, err := exec.Command(bin, args...).Output()
	lines := bytes.Split(bytes.TrimSpace(out), []byte("\n"))
	var m message
	if jerr := json.Unmarshal(lines[len(lines)-1], &m); jerr != nil || m.Summary == nil {
		return nil, errors.Join(err, jerr, errors.New("v0.1.0 printed no summary"))
	}
	return m.Summary, err
}
