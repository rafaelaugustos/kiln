# Compatibility

kiln follows semantic versioning. Until v1.0, a minor release may still break the API; the changelog
lists every such change under **Breaking**, and [Upgrading](upgrading.md) says what to do about it. From
v1.0 on, this is what holds for the whole of v1.

**The API.** Exported identifiers of package `kiln` and of `dashboard`, `kilntest`, `kilnotel`, `redisbus`,
`cron`, `memstore`, `drivertest` and the stores' `New`, `Migrate` and options are not removed or changed.
New ones may appear, and struct types may gain fields, so build structs with field names.

**The store interface.** The methods of `driver.Store` and of the interfaces it embeds do not change. A
capability added later comes as a new optional interface, the way `driver.Console` and
`driver.LimitReader` did, which kiln checks for and works without. A store written for v1.0 keeps
building, and conformance cases for a new interface skip stores that don't implement it.

**The schema.** Every schema change within v1 is additive (new tables, nullable columns, indexes) and is
applied by `New`, or by `Migrate` when the application runs with `NoMigrate`. Servers on two consecutive
minor versions run side by side during a rolling deploy; the `compat` module checks this for every
release on PostgreSQL, MySQL and SQLite. Patch releases never change the schema.

**The dashboard.** Fields of its JSON API are added, never renamed or removed. The HTML pages may change in
any release.

**Behavior.** What the documentation says is part of the API. Making the code match its documentation is a
fix, not a breaking change.

**Modules.** Every module in the repository is released together under the same version. Update them
together.

**Go.** kiln needs a Go release the Go team still supports, Go 1.27 or later for now: its code builds
structs with promoted fields, which Go 1.27 introduced. The minimum moves only in minor releases, and never
past the older of the two latest Go releases.
