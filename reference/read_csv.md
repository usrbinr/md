# Read a CSV file into a DuckDB/MotherDuck table

Loads the DuckDB **excel** extension and creates a table from a CSV file
using the `read_csv_auto()` table function. The destination is fully
qualified as `<database>.<schema>.<table>`. Only the options you supply
are forwarded to `read_csv_auto()` (e.g., `header`, `all_varchar`,
`sample_size`, `names`, `types`, `skip`, `union_by_name`,
`normalize_names`, `allow_quoted_nulls`, `ignore_errors`). If `names` or
`types` are not supplied, they are ignored. See the DuckDB
[read_csv_auto()
documentation](https://duckdb.org/docs/stable/data/csv/overview) for
more information.

## Usage

``` r
read_csv(
  .con,
  to_database_name,
  to_schema_name,
  to_table_name,
  file_path,
  header,
  all_varchar,
  sample_size,
  names,
  types,
  skip,
  union_by_name,
  normalize_names,
  allow_quoted_nulls,
  ignore_errors,
  write_type,
  ...
)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- to_database_name:

  Target database name (new or existing)

- to_schema_name:

  Target schema name (new or existing)

- to_table_name:

  Target table name to create (new or existing)

- file_path:

  Path to the Excel file (`.xlsx`)

- header:

  Logical; if `TRUE`, first row is header

- all_varchar:

  Logical; coerce all columns to `VARCHAR`

- sample_size:

  Numeric; number of rows used for type inference

- names:

  Character vector; optional column names to assign instead of reading
  from the file

- types:

  Named or unnamed character vector; column types (named preferred,
  unnamed paired to `names`)

- skip:

  Integer; number of rows to skip at the beginning of the file

- union_by_name:

  Logical; union multiple CSVs by column name

- normalize_names:

  Logical; normalize column names (lowercase, replace spaces)

- allow_quoted_nulls:

  Logical; treat `"NULL"` in quotes as NULL

- ignore_errors:

  Logical; continue on row parse errors

- write_type:

  Character; either `"overwrite"` or `"append"`, controls table creation
  behavior

- ...:

  Additional arguments passed to `read_csv_auto()` in format listed in
  duckdb documentation (optional)

## Value

Invisibly returns `NULL`. Side effect: creates
`<database>.<schema>.<table>` with the CSV data

## See also

Other db-read: [`read_excel()`](read_excel.md)
