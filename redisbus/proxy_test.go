package redisbus

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

type proxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	links  []*link
}

type link struct {
	client, server net.Conn
	muted          atomic.Bool
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *proxy) addr() string {
	return p.ln.Addr().String()
}

func (p *proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		s, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		l := &link{client: c, server: s}
		p.mu.Lock()
		p.links = append(p.links, l)
		p.mu.Unlock()
		go l.pipe(s, c)
		go l.pipe(c, s)
	}
}

func (p *proxy) mute() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.links {
		l.muted.Store(true)
	}
}

func (p *proxy) close() {
	p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, l := range p.links {
		l.close()
	}
}

func (l *link) pipe(dst, src net.Conn) {
	defer l.close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !l.muted.Load() {
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (l *link) close() {
	l.client.Close()
	l.server.Close()
}
