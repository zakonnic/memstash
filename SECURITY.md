# Security Policy

## Supported versions

The latest tagged release is supported. Fixes land on `main` and go out in the next tag.

## Reporting a vulnerability

Report privately through GitHub's
[security advisory form](https://github.com/zakonnic/memstash/security/advisories/new) - not in a public issue.

Please include a reproducer and the affected version. You can expect an acknowledgement within a few days and an
assessment of severity and fix timeline after that.

## Scope

memstash is an in-process cache library. The most relevant classes of report are memory-safety and concurrency
defects: data races, torn reads on the lock-free path, out-of-bounds access through the slot table's `unsafe`
arithmetic, or anything that lets one key's data surface under another.

Out of scope: the security of the backing store behind an L2 adapter (Redis, Postgres, ...), including its
authentication and transport - those are configured on the client you pass in. Denial of service through a
deliberately misconfigured capacity is also out of scope.
