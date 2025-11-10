# Create connection to motherduck

Establishes a connection to a MotherDuck account using DuckDB and the
MotherDuck extension. The function handles token validation, database
file creation, extension loading, and executes `PRAGMA MD_CONNECT` to
authenticate the connection.

## Usage

``` r
connect_to_motherduck(
  motherduck_token = "MOTHERDUCK_TOKEN",
  db_path = NULL,
  config
)
```

## Arguments

- motherduck_token:

  Character. Either the name of an environment variable containing your
  MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token
  itself.

- db_path:

  Character, optional. Path to a DuckDB database file or directory to
  use. If `NULL`, a temporary file is used.

- config:

  List, optional. A list of DuckDB configuration options to be passed to
  [`duckdb::duckdb()`](https://r.duckdb.org/reference/duckdb.html).

## Value

A `DBIConnection` object connected to your MotherDuck account.

## Details

This function provides a convenient interface for connecting to
MotherDuck. It allows you to:

- Use a token stored in an environment variable or supply the token
  directly.

- Optionally specify a persistent DuckDB database file or directory via
  `db_path`.

- Optionally Provide custom DuckDB configuration options via `config`.

- Automatically load the MotherDuck extension if not already loaded.

If `db_path` is not supplied, a temporary DuckDB database file will be
created in the session's temporary directory. Use `config` to pass any
DuckDB-specific options (e.g., memory limits or extensions).

## Examples

``` r
if (FALSE) { # \dontrun{
# Connect using a token stored in your .Renviron
con <- connect_to_motherduck()

# Connect with a direct token
con <- connect_to_motherduck(motherduck_token = "MY_DIRECT_TOKEN")

# Connect and specify a persistent database file
con <- connect_to_motherduck(db_path = "~/my_duckdb/md.duckdb")
} # }
```
