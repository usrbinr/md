# List all shares owned by the current user

Retrieves all database objects that are owned by the current
authenticated user in MotherDuck.

## Usage

``` r
list_owned_shares(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

A tibble with one row per share owned by the current user, including
columns for share name, object type, and granted privileges.

## Details

This function executes the `LIST SHARES;` command to return metadata
about all shares created by the current user. The returned tibble
includes details such as the share name, type of object shared, and
privileges granted.

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
[`list_shared_with_me_shares()`](https://usrbinr.github.io/md/reference/list_shared_with_me_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
owned_tbl <- list_owned_shares(con)
print(owned_tbl)
} # }
```
