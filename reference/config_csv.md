# DuckDB CSV read configuration (config_csv)

A named character list of DuckDB CSV reading configuration settings used
by the package. This includes options such as type detection, delimiter,
quote handling, sample size, and other parser flags. These reflect the
values returned by `md::config_csv`.

## Usage

``` r
config_csv
```

## Format

A named character list. Example names include:

- all_varchar:

  character; "true"/"false"

- allow_quoted_nulls:

  character; "true"/"false"

- auto_detect:

  character; "true"/"false"

- auto_type_candidates:

  character; types used for detection

- buffer_size:

  character; buffer size in bytes

- columns:

  character; column names/types struct or empty

- delim:

  character; delimiter string

- skip:

  character; number of lines to skip

## Examples

``` r
if (FALSE) { # \dontrun{
config_csv$all_varchar
} # }
```
