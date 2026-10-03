package mssqlstore

import (
	"testing"

	"github.com/rafaelaugustos/kiln/driver"
	"github.com/rafaelaugustos/kiln/drivertest"
)

func TestConformance(t *testing.T) {
	connect(t)
	drivertest.Run(t, func(t *testing.T) driver.Store { return open(t) })
}

func TestConformanceBus(t *testing.T) {
	connect(t)
	drivertest.Run(t, func(t *testing.T) driver.Store { return open(t, Bus(newBus())) })
}
