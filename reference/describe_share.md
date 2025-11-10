# Describe a MotherDuck share

Retrieves detailed metadata about a specific share in MotherDuck,
including the objects it contains, their types, and privileges granted.

## Usage

``` r
describe_share(.con, share_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- share_name:

  Character. The name of the shared path to describe.

## Value

A tibble containing metadata about the share, including object names,
types, and privileges associated with the share.

## Details

This function executes the `md_describe_database_share` system function
to obtain comprehensive information about the specified share. The
result is returned as a tibble for easy inspection and manipulation in
R.

## See also

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_or_replace_share()`](create_or_replace_share.md),
[`create_schema()`](create_schema.md),
[`create_table()`](create_table.md),
[`delete_and_create_schema()`](delete_and_create_schema.md),
[`delete_database()`](delete_database.md),
[`delete_schema()`](delete_schema.md),
[`delete_table()`](delete_table.md), [`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
share_info <- describe_share(con, "analytics.sales_share")
print(share_info)
} # }
```
