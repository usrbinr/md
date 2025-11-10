# Catalog Report Generator

This function generates a report that provides details about the current
database catalog, schema, and the number of resources (such as catalogs,
tables, and shares) that the user has access to in the connected
database. It also offers helpful functions for navigating the catalog,
schema, and databases. The report includes:

- Current database (catalog)

- Current schema

- Number of catalogs the user has access to

- Number of tables the user has access to

- Number of shares the user has access to

that help manage and explore the database resources.

## Usage

``` r
cli_show_db(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

This function doesn't return any value. It generates a formatted catalog
report as output, including the current database, schema, and access
counts for catalogs, tables, and shares.
