# DuckDB Excel read configuration (config_excel)

A named character list of DuckDB Excel reading configuration settings
used by the package. Includes options such as reading binary as string,
adding filename/row_number columns, Hive partitioning, and union by
name. These reflect the values returned by `md::config_excel`.

## Usage

``` r
config_excel
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
config_excel$binary_as_string
} # }
```
