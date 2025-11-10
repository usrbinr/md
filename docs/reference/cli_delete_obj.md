# CLI print deleted objects

This function allows you to delete specified objects (database, schema,
or table) from the connected database. The function will check if the
provided database, schema, or table exists. If they do, it will proceed
to delete them and print an action report detailing what was deleted and
how many schemas or tables were affected. If the object does not exist,
it will not delete anything.

## Usage

``` r
cli_delete_obj(.con, database_name, schema_name, table_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- database_name:

  Name of the database to be deleted (optional).

- schema_name:

  Name of the schema to be deleted (optional).

- table_name:

  Name of the table to be deleted (optional).

## Value

This function does not return any values. It prints a message indicating
the deleted objects (database, schema, or table) and the number of
schemas/tables affected by the deletion.
