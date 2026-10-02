// Package redisbus carries kiln's wakeup events over Redis Pub/Sub, for stores that cannot reach
// servers in other processes on their own: mysqlstore, and sqlitestore across processes. With a
// bus, a job enqueued by one process starts within milliseconds on a server in another, and a
// cancel reaches the server running the job without waiting for its next heartbeat.
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
//	store, err := mysqlstore.New(ctx, db, mysqlstore.Bus(redisbus.New(rdb)))
//
// Give the bus to every process that uses the store: processes that enqueue publish, and servers
// subscribe. Events are hints. Servers still poll, and Redis delivers a message only to the
// subscribers connected when it is published, so an event lost during a reconnection delays a job
// until the next poll but never loses it.
package redisbus
