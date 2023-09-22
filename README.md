# fsjkit: fsaintjacques' toolkit

A potpourri of go modules that are useful across projects.

## Modules

- `tx`: The transaction module adds various wrappers over `sql.Tx`, notably
  `tx.Transactor` a transaction manager.
- `mailbox`: The mailbox module expose the `Mailbox` and `Consumer`, which allows
  implementing the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) and more.

## Testing

Tests (unit, integration) are found in the e2e module; the goal is to
minimize the module dependencies. Because go modules doesn't support specifying
testing dependencies, this module takes that hit. It is not meant to be imported by
external packages.  For example, most tests depends on [testify](https://github.com/stretchr/testify) or
[dockertest](https://github.com/ory/dockertest) which can pull a lot of dependencies.
