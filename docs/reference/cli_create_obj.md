# Database, Schema, and Table Creator

This function creates or inserts data into a specified database, schema,
and table. If no database, schema, or table is provided, the function
attempts to use the current database, schema, or table. It first checks
if the provided database, schema, or table exists, and then either
creates a new one or inserts into the existing one, based on the given
parameters. It generates an action report indicating the status of the
operation, whether a new database, schema, or table was created or
whether existing ones were used.

## Usage

``` r
cli_create_obj(.con, database_name, schema_name, table_name, write_type)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  The name of the database to create or insert into. If missing, the
  current database is used.

- schema_name:

  The name of the schema to create or insert into. If missing, the
  current schema is used.

- table_name:

  The name of the table to create or insert into. If missing, no
  table-specific action is taken.

- write_type:

  Specifies the type of write operation. Used to describe whether an
  existing table is updated (e.g., "insert" or "update").

## Value

This function doesn't return any value. It generates a formatted report
showing whether a new database, schema, or table was created or if
existing ones were used.
