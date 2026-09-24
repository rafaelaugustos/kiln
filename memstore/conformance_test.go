package memstore_test

import (
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
	"github.com/rafaelaugustos/kiln/memstore"
)

func TestConformance(t *testing.T) {
	drivertest.Run(t, func(*testing.T) driver.Store { return memstore.New() })
}
