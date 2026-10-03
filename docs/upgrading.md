# Upgrading

Every release is tested against the previous one on the same database: the `compat` module runs the
published version and the new code side by side while jobs move between them. Schema changes only ever
add things, so servers on two consecutive versions can run together during a rolling deploy.

v0.3 adds the rate limit columns. During a rolling deploy from v0.2, jobs on a key with a `Rate` and no
`Max` are released only by servers already running v0.3, at the pace the rate allows; once released, any
server may run them. On a key with both, v0.2 servers still enforce `Max` but not the rate until they are
upgraded. Everything else is processed by both versions.

Additive changes are recorded in a `schema_changes` table next to the jobs tables. If the database role
kiln runs with was granted privileges table by table, grant it the same on `schema_changes` after the
upgrade.

v0.4 adds a nullable `admit_tat` column to the limits table. Until every server runs v0.4, the ones
still on v0.3 release jobs whose start time has come without the check that keeps them from starting
together after a stall.

v0.5 has no schema changes for the existing stores. `mssqlstore` is new in it.

v0.6 adds two changes, both additive: a log table and a `progress` column for the job console
(`003_console`), and a nullable group column on recurring jobs (`004_recurring_group`). On SQL Server
Standard edition, adding the `progress` column with its default may touch every row of the jobs and
archive tables; on Enterprise, Developer and Azure SQL it only changes metadata.

v0.7 adds one additive change, `005_job_extras`: a nullable `title` column on the jobs and archive tables.

v0.8 adds one additive change, `006_nested_batches`: a nullable `parent_id` column on batches and two
indexes. v0.7 servers don't know about nested batches and can finish a batch while batches nested in it
are still running, so nest batches only once every server runs v0.8.
