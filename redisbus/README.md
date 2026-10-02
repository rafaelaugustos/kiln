# redisbus

Redis Pub/Sub wakeups for [kiln](https://github.com/rafaelaugustos/kiln) stores that have no notifications
of their own.

`pgstore` wakes servers with `LISTEN`/`NOTIFY`. MySQL has nothing like it and SQLite can only notify servers
in the same process, so with `mysqlstore` and `sqlitestore` a server in another process finds new work on
its next poll. A `redisbus.Bus` carries the same wakeups over Redis: a job enqueued by one process starts at
once on a server in another, a cancel reaches the server running the job immediately, and a resumed queue
is picked up without waiting for the next poll.

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})

store, err := mysqlstore.New(ctx, db, mysqlstore.Bus(redisbus.New(rdb)))
```

```go
store, err := sqlitestore.New(ctx, db, sqlitestore.Bus(redisbus.New(rdb)))
```

Give the bus to every process that uses the store: processes that enqueue publish, servers subscribe. A
process without it keeps working; its enqueues are simply found by polling.

## Channels

`redisbus.Channel("billing")` sets the Pub/Sub channel, `kiln` by default. Use one channel per kiln
installation, the same way each installation has its own tables. Two installations that share a Redis and a
channel still run correctly, but every enqueue in one wakes the servers of the other for nothing.

## Clients

`New` takes any `redis.UniversalClient`:

- standalone: `redis.NewClient`;
- Sentinel: `redis.NewFailoverClient`, or `redis.NewUniversalClient` with `MasterName`;
- Cluster: `redis.NewClusterClient`, or `redis.NewUniversalClient` with several addresses. `PUBLISH` goes to
  any node and the cluster forwards it to every node; each subscriber keeps one connection to the node that
  owns the channel's slot.

Set `ContextTimeoutEnabled: true` on the client so that the deadline of the context passed to `Publish` also
bounds network reads and writes. go-redis ignores context deadlines for those otherwise, and a Redis that
accepts connections but stops answering can then hold a publish for up to `ReadTimeout` (3 seconds by
default). Dialing and waiting for a pooled connection follow the context either way.

Each `Subscribe` opens one dedicated connection. Close the Redis client after the kiln servers have stopped:
on a closed client `Subscribe` returns an error wrapping `redis.ErrClosed`.

## Delivery

Events are hints, and correctness never depends on them:

- Servers claim jobs from the database, never from a message. A wakeup only makes a server look sooner.
- Cancellations and paused queues also reach servers through their heartbeats.
- Polling stays on. Servers still fetch every `PollInterval` and promote due jobs at least as often.

Redis Pub/Sub delivers a message only to subscribers connected at that moment, so events published while a
server reconnects are lost, and nothing is acknowledged or retried. A subscriber also never lets a slow
consumer stall its connection: it keeps reading and drops events once 1024 are waiting. To make up for
missed events it reports a `Resync` after every subscription and after dropping anything, and a server
answers a `Resync` with a heartbeat and a fetch from all of its queues.

## Failures

- `Publish` returns an error when Redis cannot be reached. A store never fails the write that triggered it:
  the job is already committed, and servers find it by polling.
- A subscriber reconnects with a jittered backoff that grows from 50ms to 5s. The backoff starts over only
  after a subscription has held for 5 seconds, so a connection that drops right after every subscribe
  cannot make every server resync against the database many times a second.
- A subscription that has been quiet for 5 seconds sends a `PING`, and reconnects when nothing comes back
  within 5 more seconds. That catches connections that died without being closed, such as flows dropped by
  a NAT or a load balancer and servers that hang, and keeps idle connections alive through them.
- After a Sentinel failover of a master that is still running, go-redis leaves the subscription on the old
  master. It now replicates from the new master, so events keep arriving; those published during the switch
  are lost. When the old master is down or hung, the ping moves the subscription to the new master.

## Wire format

One `Publish` call is one message, with a record per event: a kind byte, a uvarint payload length and the
payload, which is the queue name for `JobsReady` and `QueueChanged`, the uvarint job id for
`CancelRequested` and empty for `Resync`. A `JobsReady` for queue `mail` takes 6 bytes. Encoding allocates
nothing, so a publish costs one `PUBLISH` round trip and go-redis's own allocations. Subscribers skip kinds
they do not know, so a newer release can add kinds without confusing older servers, and they drop what they
cannot parse.

## Tests

The tests use the Redis at `localhost:56379`, or `KILN_REDIS_ADDR`, and skip when it cannot be reached.
