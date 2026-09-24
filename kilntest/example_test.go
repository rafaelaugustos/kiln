package kilntest_test

import (
	"context"
	"fmt"

	"github.com/rafaelaugustos/kiln"
	"github.com/rafaelaugustos/kiln/kilntest"
)

type ResizeImage struct {
	Path string
}

func (ResizeImage) Kind() string { return "resize_image" }

func ExampleWork() {
	mux := kiln.NewMux()
	kiln.Handle(mux, func(ctx context.Context, j *kiln.Job[ResizeImage]) error {
		if j.Args.Path == "" {
			return kiln.Permanent(fmt.Errorf("no path"))
		}
		return nil
	})

	good := kilntest.Work(context.Background(), mux, ResizeImage{Path: "photo.png"})
	fmt.Println(good.State, good.Err)

	bad := kilntest.Work(context.Background(), mux, ResizeImage{})
	fmt.Println(bad.State, bad.Err)

	// Output:
	// succeeded <nil>
	// failed no path
}
