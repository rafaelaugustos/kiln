package main

import (
	"context"
	"log/slog"
	"os"
)

type job struct {
	Seq int `json:"seq,omitempty"`
}

func (job) Kind() string { return "bench" }

type handler func(ctx context.Context, seq int)

type system interface {
	open(ctx context.Context, url, schema string) error
	close()
	insertMany(ctx context.Context, n int) error
	insert(ctx context.Context, seq int) error
	start(ctx context.Context, workers int, h handler) error
	ready(ctx context.Context) error
	stop(ctx context.Context) error
	table() string
	completed() string
}

type variant struct {
	id   string
	name string
	new  func() system
}

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
