# List MotherDuck/DuckDB Extensions

Retrieves all available DuckDB or MotherDuck extensions along with their
descriptions, installation and load status.

## Usage

``` r
list_extensions(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A tibble with one row per extension and columns describing its metadata
and current status.

## Details

The `list_extensions()` function queries the database for all extensions
that are available in the current DuckDB or MotherDuck connection. The
returned tibble includes information such as:

- `extension_name`: Name of the extension.

- `description`: Short description of the extension.

- `installed`: Logical indicating if the extension is installed.

- `loaded`: Logical indicating if the extension is currently loaded.

This is useful for determining which extensions can be installed or
loaded using
[`install_extensions()`](https://usrbinr.github.io/md/reference/install_extensions.md)
or
[`load_extensions()`](https://usrbinr.github.io/md/reference/load_extensions.md).

## See also

Other db-list:
[`list_all_databases()`](https://usrbinr.github.io/md/reference/list_all_databases.md),
[`list_all_tables()`](https://usrbinr.github.io/md/reference/list_all_tables.md),
[`list_current_schemas()`](https://usrbinr.github.io/md/reference/list_current_schemas.md),
[`list_current_tables()`](https://usrbinr.github.io/md/reference/list_current_tables.md),
[`list_fns()`](https://usrbinr.github.io/md/reference/list_fns.md),
[`list_setting()`](https://usrbinr.github.io/md/reference/list_setting.md),
[`list_shares()`](https://usrbinr.github.io/md/reference/list_shares.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())

# List all available extensions
list_extensions(con)

DBI::dbDisconnect(con)
} # }
```
