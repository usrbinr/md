# Convert Table Metadata to SQL Identifiers

Converts a tibble of table metadata (`table_catalog`, `table_schema`,
`table_name`) into a list of
[`DBI::Id`](https://dbi.r-dbi.org/reference/Id.html) SQL identifiers.
Useful for safely quoting fully qualified table references in
`DBI`/`dbplyr` workflows.

## Usage

``` r
convert_table_to_sql_id(x)
```

## Arguments

- x:

  A tibble or data frame containing the columns:

  - `table_catalog` — database/catalog name

  - `table_schema` — schema name

  - `table_name` — table name

## Value

A list of [`DBI::Id`](https://dbi.r-dbi.org/reference/Id.html) objects,
each representing a fully-qualified table.
