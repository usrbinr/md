# Validate Installed MotherDuck/DuckDB Extensions

Checks whether specified DuckDB or MotherDuck extensions are installed
and provides a detailed status report.

## Usage

``` r
validate_extension_install_status(.con, extension_names, return_type = "msg")
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- extension_names:

  A character vector of extensions to validate.

- return_type:

  One of `"msg"`, `"ext"`, or `"arg"`. Determines the type of return
  value.

  - `"msg"` prints CLI messages.

  - `"ext"` returns a list of installed and failed extensions.

  - `"arg"` returns TRUE if all requested extensions are installed,
    FALSE otherwise.

## Value

Depending on `return_type`:

- `"msg"`: prints CLI messages (invisible `NULL`).

- `"ext"`: list with `success_ext` and `fail_ext`.

- `"arg"`: logical indicating if all requested extensions are installed.

## Details

The `validate_extension_install_status()` function validates the current
connection and checks which of the requested extensions are installed.
It produces a detailed CLI report showing which extensions are
installed, not installed, or missing.

The function can return different outputs based on the `return_type`
argument:

- `"msg"`: prints a CLI report with extension statuses.

- `"ext"`: returns a list containing `success_ext` (installed) and
  `fail_ext` (not installed).

- `"arg"`: returns a logical value indicating whether all requested
  extensions are installed.

## See also

Other db-con: [`install_extensions()`](install_extensions.md),
[`load_extensions()`](load_extensions.md),
[`show_motherduck_token()`](show_motherduck_token.md),
[`validate_extension_load_status()`](validate_extension_load_status.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())

# Print CLI report
validate_extension_install_status(con, extension_names = c("arrow", "excel"), return_type = "msg")

# Return a list of installed and failed extensions
validate_extension_install_status(con, extension_names = c("arrow", "excel"), return_type = "ext")

# Return logical indicating if all requested extensions are installed
validate_extension_install_status(con, extension_names = c("arrow", "excel"), return_type = "arg")
} # }
```
