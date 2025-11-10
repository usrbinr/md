# Load and Install DuckDB/MotherDuck Extensions

Installs (if necessary) and loads valid DuckDB or MotherDuck extensions
for the active connection.

## Usage

``` r
load_extensions(.con, extension_names)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- extension_names:

  A character vector of DuckDB/MotherDuck extension names to
  load/install.

## Value

Invisibly returns `NULL`. The function prints a CLI report of the
extension status.

## Details

The `load_extensions()` function first validates the provided
DuckDB/MotherDuck connection, then checks which of the requested
extensions are valid and not already installed. Valid extensions are
installed and loaded into the current session. Invalid extensions are
reported to the user. The function provides a detailed CLI report
summarizing which extensions were successfully installed and loaded, and
which were invalid.

It is especially useful for ensuring that required extensions, such as
`motherduck`, are available in your database session. The CLI messages
also provide guidance on listing all available extensions and installing
additional DuckDB extensions.

## See also

Other db-con: [`install_extensions()`](install_extensions.md),
[`show_motherduck_token()`](show_motherduck_token.md),
[`validate_extension_install_status()`](validate_extension_install_status.md),
[`validate_extension_load_status()`](validate_extension_load_status.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())

# Install and load the 'motherduck' extension
load_extensions(con, "motherduck")

# Load multiple extensions
load_extensions(con, c("motherduck", "httpfs"))

DBI::dbDisconnect(con)
} # }
```
