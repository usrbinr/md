# Validate Loaded MotherDuck/DuckDB Extensions

Checks whether specified DuckDB or MotherDuck extensions are loaded in
the current session and provides a detailed status report.

## Usage

``` r
validate_extension_load_status(.con, extension_names, return_type = "msg")
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- extension_names:

  A character vector of extensions to validate.

- return_type:

  One of `"msg"`, `"ext"`, or `"arg"`. Determines the type of return
  value:

  - `"msg"` prints CLI messages.

  - `"ext"` returns a list with `success_ext`, `fail_ext`, and
    `missing_ext`.

  - `"arg"` returns TRUE if all requested extensions are loaded, FALSE
    otherwise.

## Value

Depending on `return_type`:

- `"msg"`: prints CLI messages (invisible `NULL`).

- `"ext"`: list with `success_ext`, `fail_ext`, and `missing_ext`.

- `"arg"`: logical indicating if all requested extensions are loaded.

## Details

The `validate_extension_load_status()` function validates the current
connection, then checks which of the requested extensions are loaded. It
produces a detailed CLI report showing which extensions are loaded,
failed to load, or missing.

Depending on the `return_type` argument, the function can either print
messages, return a list of extension statuses, or return a logical value
indicating whether all requested extensions are successfully loaded.

## See also

Other db-con: [`install_extensions()`](install_extensions.md),
[`load_extensions()`](load_extensions.md),
[`show_motherduck_token()`](show_motherduck_token.md),
[`validate_extension_install_status()`](validate_extension_install_status.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())

# Print CLI report
validate_extension_load_status(con, extension_names = c("excel", "arrow"), return_type = "msg")

# Return a list of loaded, failed, and missing extensions
validate_extension_load_status(con, extension_names = c("excel", "arrow"), return_type = "ext")

# Return logical indicating if all requested extensions are loaded
validate_extension_load_status(con, extension_names = c("excel", "arrow"), return_type = "arg")
} # }
```
