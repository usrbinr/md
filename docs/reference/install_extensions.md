# Install DuckDB/MotherDuck Extensions

Installs valid DuckDB or MotherDuck extensions for the current
connection.

## Usage

``` r
install_extensions(.con, extension_names)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- extension_names:

  A character vector of DuckDB/MotherDuck extension names to install.

## Value

Invisibly returns `NULL`. A detailed CLI report of installation
success/failure is printed.

## Details

The `install_extensions()` function validates the provided
DuckDB/MotherDuck connection, then checks which of the requested
extensions are valid. Valid extensions that are not already installed
are installed using the `INSTALL` SQL command. Invalid extensions are
reported to the user via CLI messages. This function provides a summary
report describing which extensions were successfully installed and which
were invalid.

Unlike [`load_extensions()`](load_extensions.md), this function focuses
purely on installation and does not automatically load extensions after
installing.

## See also

Other db-con: [`load_extensions()`](load_extensions.md),
[`show_motherduck_token()`](show_motherduck_token.md),
[`validate_extension_install_status()`](validate_extension_install_status.md),
[`validate_extension_load_status()`](validate_extension_load_status.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())

# Install the 'motherduck' extension
install_extensions(con, "motherduck")

# Install multiple extensions
install_extensions(con, c("fts", "httpfs"))

DBI::dbDisconnect(con)
} # }
```
