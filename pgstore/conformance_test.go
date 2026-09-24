package pgstore

import (
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
)

func TestConformance(t *testing.T) {
	connect(t).Close()
	drivertest.Run(t, func(t *testing.T) driver.Store { return open(t) })
}
