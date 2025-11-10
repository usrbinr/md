# Validate Mother Duck Connection Status

Validates if you are successfully connected to motherduck database and
will return either a logical value or print a message

## Usage

``` r
validate_md_connection_status(.con, return_type = "msg")
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- return_type:

  return message or logical value of connection status

## Value

logical value or warning message

## See also

[`connect_to_motherduck()`](connect_to_motherduck.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())
validate_md_connection_status(con)
} # }
```
