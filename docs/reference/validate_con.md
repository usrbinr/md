# Validate connection is DuckDB

Validates that your connection object is a DuckDB connection

## Usage

``` r
validate_con(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

logical value or error message

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())
validate_duckdb_con(con)
} # }
```
