# Show Your MotherDuck Token

Displays the active MotherDuck authentication token associated with the
current connection. Useful for debugging or verifying that your session
is authenticated correctly.

## Usage

``` r
show_motherduck_token(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A tibble containing the current MotherDuck token.

## Details

The `show_motherduck_token()` function executes the internal MotherDuck
pragma `print_md_token` and returns the token information. This function
should only be used in secure environments, as it exposes your
authentication token in plain text. It requires a valid MotherDuck
connection established with
[`DBI::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html).

## See also

Other db-con:
[`install_extensions()`](https://usrbinr.github.io/md/reference/install_extensions.md),
[`load_extensions()`](https://usrbinr.github.io/md/reference/load_extensions.md),
[`validate_extension_install_status()`](https://usrbinr.github.io/md/reference/validate_extension_install_status.md),
[`validate_extension_load_status()`](https://usrbinr.github.io/md/reference/validate_extension_load_status.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "md:")
show_motherduck_token(con)
DBI::dbDisconnect(con)
} # }
```
