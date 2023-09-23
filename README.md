# fsjkit: fsaintjacques' toolkit

A potpourri of go modules that are useful across projects.

## Modules

- `tx`: The transaction module adds various wrappers over `sql.Tx`, notably
  `tx.Transactor` a transaction manager. The transactor supports savepoint with transaction
  enabling partial rollback of savepoints.
- `mailbox`: The mailbox module expose the `Mailbox` and `Consumer`, which allows
  implementing the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) and more.
  Multiple middlewares offer robustness via retries, timeouts and dead-letter pattern.

### tx: Improved Transactions

The `tx` module provides a `Transactor` interface that wraps `sql.Tx` and adds
savepoints and recursion support. The `Transactor` interface is implemented by 
`sql.DB` and `sql.Conn`. The transactor exposes a single `InTx` method that takes 
a closure that will be executed in a transaction. If the closure returns an error,
the transaction will be rolled back. If the closure returns nil, the transaction
will be committed. If the closure panics, the transaction will be rolled back and
the panic will be rethrown. If `InTx` is called again (recursively) in the closure,
it will re-use the existing transaction instead of creating a new one, this is done
transparently via the `context.Context`.

```go
func (r *Repo) Get(ctx context.Context, id int) (*User, error) {
    var user User
    if err := r.transactor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
        stmt := "SELECT id, name FROM users WHERE id = $1"
        if err := tx.QueryRowContext(ctx, stmt, id).Scan(&user.ID, &user.Name); err != nil {
            return err
        }

        return nil
    }); err != nil {
        return nil, err
    }

    return &user, nil
}

func (r *Repo) Update(ctx context.Context, id int, name string) error {
    return r.transactor.InTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
        // This will transparently re-use the existing transaction
        user, err := r.Get(ctx, id)
        if err != nil {
            return err
        }

        const stmt = "UPDATE users SET name = $1 WHERE id = $2"
        if _, err := tx.ExecContext(ctx, stmt, name, id); err != nil {
            return err
        }

        return nil
    })
}
```

The `Transactor` can be instantiated with the `WithSavepoint` option, which will
enable savepoints. Savepoints are useful to rollback only a part of a transaction
instead of the whole thing. This is useful when you want to do multiple operations
in a single transaction, but you want to be able to rollback only a part of it.
This can be particuarly useful in integration testing, here's an example:

```go
func TestMySvc(t *testing.T) {
  db, err := sql.Open("postgres", "...")
  transactor := tx.NewTransactor(db, tx.WithSavepoint())
  // Assume that svc uses transactor.InTx in its methods
  svc = NewFooSvc(transactor)

  err := transactor.InTx(ctx, func(context.Context, tx *sql.Tx) error {
    t.Run("test1", func(t *testing.T) {
      // If CreateFoo fails, the savepoint opened inside will be rolled back.
      // This will *not* cancel the transaction. This is useful if CreateFoo
      // inserts data in a table that is used by other tests. If savepoints 
      // weren't used, either the whole transaction would be rolled back, or
      // the data would be left in the database and affect other tests.
      svc.CreateFoo(ctx, "bar")
    })

    t.Run("test2", func(t *testing.T) {
      // No side effects of test1 will be visible here (assuming CreateFoo failed)
      ...
    })

    // The side effects of test1 and test2 will be rolled back and not visible to
    // other tests.
    return errors.New("rollback")
  })
}
```

### mailbox: Transactional outbox

The `mailbox` module provides a facility to implement the outbox pattern; it allows
deferring asynchronous operations to a later time in a transactional manner. For example,
triggering a webhook or sending an email. The module provides a `Mailbox` interface to put
messages in the outbox, and a `Consumer` interface to consume messages from the outbox.

Messages are always put in the outbox in the context of a transaction. A `Consumer` runs
in a separate goroutine and consumes messages from the outbox. The `Consumer` is responsible
for executing the message and marking it as done. If the message fails, it can be retried
later.

## Testing

Tests (unit, integration) are found in the e2e module; the goal is to
minimize the module dependencies. Because go modules doesn't support specifying
testing dependencies, this module takes that hit. It is not meant to be imported by
external packages.  For example, most tests depends on [testify](https://github.com/stretchr/testify) or
[dockertest](https://github.com/ory/dockertest) which can pull a lot of dependencies.
