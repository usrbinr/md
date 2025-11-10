# Overwrite or Append a Local Tibble to a Database Table

Takes an in-memory tibble (or data frame) and writes it to a database
table using a `DBI` connection. The function supports both **overwrite**
and **append** modes, automatically creates the target database and
schema if they do not exist, and adds audit fields (`upload_date`,
`upload_time`) to the written table.

## Usage

``` r
create_table_tbl(
  .data,
  .con,
  database_name,
  schema_name,
  table_name,
  write_type = "overwrite"
)
```

## Arguments

- .data:

  A tibble or data frame to be written to the database.

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Name of the database to create/use. If missing, the current database
  of the connection will be used.

- schema_name:

  Name of the schema to create/use. If missing, the current schema of
  the connection will be used.

- table_name:

  Name of the table to create or append to.

- write_type:

  Write strategy: either `"overwrite"` (drop/create) or `"append"`
  (insert rows). Defaults to `"overwrite"`.

## Value

Invisibly returns `NULL`. Side effect: writes the tibble to the
specified database table.

## Details

- If the connection is a MotherDuck connection (detected by
  [`validate_md_connection_status()`](validate_md_connection_status.md)),
  the function ensures the database is created and switches to it before
  creating the schema.

- Two audit columns are added to the data before writing: `upload_date`
  (date of run) and `upload_time` (time and timezone of run).

- Uses [`DBI::Id()`](https://dbi.r-dbi.org/reference/Id.html) to
  explicitly target the database/schema/table.

- `write_type = "overwrite"` will drop and recreate the table.

- `write_type = "append"` will insert rows into an existing table.
