# fsjkit: fsaintjacques' toolkit

A potpourri of go modules that are useful to use across projects.

## Modules

- `tx`: Facility to manipulate transactions


## Testing

Tests (unit, integration) are found in the e2e directory; this is to
minimize the module dependencies. Thus importing one of the public
module will not transitively add the dependencies required for testing. For 
example, some tests depends on [testify](https://github.com/stretchr/testify) or 
[sqlmock](https://github.com/DATA-DOG/go-sqlmock) which themselves pulls a lot of 
dependencies.