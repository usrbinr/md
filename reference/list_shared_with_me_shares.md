# List all shares shared with the current user

Retrieves all database objects that have been shared with the current
authenticated user in MotherDuck.

## Usage

``` r
list_shared_with_me_shares(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A tibble containing one row per shared object, with columns describing
the owner, object type, object name, and granted privileges.

## Details

This function queries the `MD_INFORMATION_SCHEMA.SHARED_WITH_ME` view to
return metadata about all shares granted to the current user, including
the owner, object name, type, and privileges. The result is returned as
a tidy tibble for easy manipulation in R.

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
[`delete_table()`](delete_table.md),
[`describe_share()`](describe_share.md),
[`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
shared_tbl <- list_shared_with_me_shares(con)
print(shared_tbl)
} # }
```
