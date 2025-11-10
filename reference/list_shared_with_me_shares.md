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

Other db-manage:
[`alter_table_schema()`](https://usrbinr.github.io/md/reference/alter_table_schema.md),
[`copy_tables_to_new_location()`](https://usrbinr.github.io/md/reference/copy_tables_to_new_location.md),
[`create_database()`](https://usrbinr.github.io/md/reference/create_database.md),
[`create_if_not_exists_share()`](https://usrbinr.github.io/md/reference/create_if_not_exists_share.md),
[`create_or_replace_share()`](https://usrbinr.github.io/md/reference/create_or_replace_share.md),
[`create_schema()`](https://usrbinr.github.io/md/reference/create_schema.md),
[`create_table()`](https://usrbinr.github.io/md/reference/create_table.md),
[`delete_and_create_schema()`](https://usrbinr.github.io/md/reference/delete_and_create_schema.md),
[`delete_database()`](https://usrbinr.github.io/md/reference/delete_database.md),
[`delete_schema()`](https://usrbinr.github.io/md/reference/delete_schema.md),
[`delete_table()`](https://usrbinr.github.io/md/reference/delete_table.md),
[`describe_share()`](https://usrbinr.github.io/md/reference/describe_share.md),
[`drop_share()`](https://usrbinr.github.io/md/reference/drop_share.md),
[`list_owned_shares()`](https://usrbinr.github.io/md/reference/list_owned_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
shared_tbl <- list_shared_with_me_shares(con)
print(shared_tbl)
} # }
```
