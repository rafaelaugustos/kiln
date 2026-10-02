# Security

## Reporting a vulnerability

Please don't open a public issue. Report it privately through the repository's
[Security tab](https://github.com/rafaelaugustos/kiln/security/advisories/new) ("Report a vulnerability"),
with what you found, the versions affected and, if you can, a way to reproduce it.

You'll get an answer within a few days. Once a fix is released, the advisory is published with credit to
you, unless you'd rather not be named.

## Supported versions

kiln is before 1.0, so fixes go into the latest release only. Upgrading to it is the way to get them.

## Scope

Some parts of kiln deal with untrusted input and are worth extra attention:

- the dashboard, which runs whatever `Options.Authorize` allows and exposes a JSON API that changes state;
- job args, meta and output, which the dashboard displays and handlers decode;
- the stores' SQL, which is built from the options an application passes in, such as schema names and
  table prefixes.
