# Read an Excel file into a DuckDB/MotherDuck table

Loads the DuckDB **excel** extension and creates a table from an Excel
file using the `read_xlsx()` table function. The destination is fully
qualified as `<database>.<schema>.<table>`. Only the options you supply
are forwarded to `read_xlsx()` (e.g., `sheet`, `header`, `all_varchar`,
`ignore_errors`, `range`, `stop_at_empty`, `empty_as_varchar`). See
'duckdb extension
[read_csv](https://duckdb.org/docs/stable/core_extensions/excel) for
more information

## Usage

``` r
read_excel(
  .con,
  to_database_name,
  to_schema_name,
  to_table_name,
  file_path,
  header,
  sheet,
  all_varchar,
  ignore_errors,
  range,
  stop_at_empty,
  empty_as_varchar,
  write_type
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

- sheet:

  Character; sheet name to read (defaults to first sheet)

- all_varchar:

  Logical; coerce all columns to `VARCHAR`

- ignore_errors:

  Logical; continue on cell/row errors

- range:

  Character; Excel range like `"A1"` or `"A1:C100"`

- stop_at_empty:

  Logical; stop at first completely empty row

- empty_as_varchar:

  Logical; treat empty columns as `VARCHAR`

- write_type:

  Logical, will drop previous table and replace with new table

## Value

Invisibly returns `NULL`. Side effect: creates
`<database>.<schema>.<table>` with the Excel data.

## See also

Other db-read:
[`read_csv()`](https://usrbinr.github.io/md/reference/read_csv.md)
