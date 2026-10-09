# Unique jobs

`Unique{Key}` makes `Enqueue` return the id of a live job with the same kind and key instead of
inserting another one. The key is released as soon as that job succeeds, fails or is deleted.

`Unique{Key, For: d}` holds the key for `d` from the first enqueue, whatever happens to the job in the
meantime, including success. It means "at most once per `d`" (one reminder per 10 minutes), not "no
duplicates while it runs".

`Unique{Key, Replace: true}` updates a holder that hasn't started yet with the new args, meta, tags, title
and priority, so the job runs with the latest data. `Unique{Key, Debounce: d}` runs the job `d` after the
last enqueue: every new enqueue pushes a holder that is still waiting and replaces its args, which suits
work like "reindex once the user stops editing". A holder that is already running is never touched.
