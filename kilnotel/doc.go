// Package kilnotel instruments kiln with OpenTelemetry. [EnqueueMiddleware] traces inserts and
// hands their trace context to the jobs, [Middleware] runs each attempt in a span that continues
// that trace and records job metrics, and [Observe] reports gauges for a server and a store:
//
//	client := kiln.NewClient(store, kilnotel.EnqueueMiddleware())
//	mux.Use(kilnotel.Middleware())
//	unregister, err := kilnotel.Observe(server, store)
//
// It depends only on the OpenTelemetry API and reports through whatever SDK the application
// configures, using the global tracer provider, meter provider and propagator unless options say
// otherwise. The global propagator does nothing until one is set with otel.SetTextMapPropagator,
// and without one a job's trace ends at its enqueue span.
package kilnotel
