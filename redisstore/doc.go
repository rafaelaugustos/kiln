// Package redisstore keeps kiln's jobs in Redis, through go-redis v9, for applications that would
// rather not run their queue on their database. [New] returns a store:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
//	store, err := redisstore.New(ctx, rdb)
//	client := kiln.NewClient(store)
//
// It needs Redis 7.0 or later, or a server that speaks the same protocol, such as Valkey. Jobs,
// queues, limits, batches, recurring jobs and the dashboard's data all live in Redis, under keys
// that start with "{kiln}:" unless [Prefix] names another prefix.
//
// # How it works
//
// The store loads its code into the server as a function library and every method is one call of
// it. Redis runs a call alone, from start to finish, so a call takes effect all at once: there are
// no row locks, no deadlocks and no [driver.Busy] outcomes, and an insert of many jobs is atomic
// however large. The price is that nothing else runs meanwhile, so the store bounds the work of
// its own maintenance calls, and an insert, a claim or a finish holds the server for about as long
// as its number of jobs.
//
// Time comes from the server's clock, read inside each call. Servers learn of new work from
// events the calls publish on a Pub/Sub channel named after the prefix; see [Store.Subscribe].
//
// # Durability
//
// A job is as safe as the server's persistence makes it. With the append-only file fsynced every
// second, the default of most deployments that turn it on, a crash of the server can lose the
// last second of writes: jobs just enqueued disappear and jobs just finished run again. With
// appendfsync always nothing acknowledged is lost, at the cost of a disk write per call. Without
// persistence a restart loses every job. A database store keeps the stronger promise; choose this
// one for the load it takes off the database and for its speed.
//
// The store must never lose keys to eviction, so [New] refuses a server whose maxmemory-policy
// evicts any key. Finished jobs stay in memory until they are pruned, which makes
// [github.com/rafaelaugustos/kiln.ServerConfig.Retention] the setting that sizes the server.
//
// # Transactions
//
// Redis is not the application's database, so there is no writer bound to the application's
// transaction: a job enqueued here is not rolled back with it. [Store.InTx] groups the store's own
// writes, which is what package kiln uses to insert a batch together with its jobs.
//
// # Redis Cluster
//
// Every key of a store carries the same hash tag, so a store lives in one slot and works on a
// cluster, without spreading over its nodes. Stores with different prefixes land in different
// slots.
package redisstore
