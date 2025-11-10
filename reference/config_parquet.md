# DuckDB Parquet read configuration (config_parquet)

A named character list of DuckDB Parquet reading configuration settings
used by the package. Includes options such as binary encoding, filename
columns, row numbers, and union by name. Reflects `md::config_parquet`.

## Usage

``` r
config_parquet
```

## Format

A named character list. Example names include:

- binary_as_string:

  character; "true"/"false"

- encryption_config:

  character; encryption struct or "-" if none

- filename:

  character; "true"/"false"

- file_row_number:

  character; "true"/"false"

- hive_partitioning:

  character; e.g., "(auto-detect)"

- union_by_name:

  character; "true"/"false"

## Examples

``` r
if (FALSE) { # \dontrun{
config_parquet$binary_as_string
} # }
```
