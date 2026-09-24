package kiln

import "github.com/rafaelaugustos/kiln/driver"

type (
	State     = driver.State
	Record    = driver.Record
	Entry     = driver.Entry
	Inserted  = driver.Inserted
	Filter    = driver.Filter
	JobQuery  = driver.JobQuery
	Page      = driver.Page
	Retention = driver.Retention
)

const (
	Awaiting   = driver.Awaiting
	Scheduled  = driver.Scheduled
	Throttled  = driver.Throttled
	Enqueued   = driver.Enqueued
	Processing = driver.Processing
	Succeeded  = driver.Succeeded
	Failed     = driver.Failed
	Deleted    = driver.Deleted
)

type Args interface {
	Kind() string
}

type Spec struct {
	Args    Args
	Options []InsertOption
}
