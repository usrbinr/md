# Launch the DuckDB UI in your browser

The `launch_ui()` function installs and launches the DuckDB UI extension
for an active DuckDB database connection. This allows users to interact
with the database via a web-based graphical interface.

The function will check that the connection is valid before proceeding.

## Usage

``` r
launch_ui(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

The function is called for its side effects and does not return a value.
It launches the DuckDB UI and opens it in your default web browser.

## Details

The function performs the following steps:

- Checks that the provided DuckDB connection is valid. If the connection
  is invalid, it aborts with a descriptive error message.

- Installs the `ui` extension into the connected DuckDB instance.

- Calls the `start_ui()` procedure to launch the DuckDB UI in your
  browser.

This provides a convenient way to explore and manage DuckDB databases
interactively without needing to leave the R environment.

## See also

Other db-meta: [`cd()`](cd.md), [`pwd()`](pwd.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect to DuckDB
con_db <- DBI::dbConnect(duckdb::duckdb())

# Launch the DuckDB UI
launch_ui(con_db)

# Clean up
DBI::dbDisconnect(con_db, shutdown = TRUE)
} # }
```
