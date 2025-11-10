# Create or replace a MotherDuck database share

Creates a new share or replaces an existing share for a specified
database in MotherDuck. This allows you to update the configuration of
an existing share or create a new one if it does not exist.

## Usage

``` r
create_or_replace_share(
  .con,
  share_name,
  database_name,
  access = "PUBLIC",
  visibility = "LISTED",
  update = "AUTOMATIC"
)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- share_name:

  Character. The name of the share to create or replace.

- database_name:

  Character. The name of the database to be shared.

- access:

  Character. Access level for the share; either `"RESTRICTED"` or
  `"PUBLIC"` (default: `"PUBLIC"`).

- visibility:

  Character. Visibility of the share; either `"HIDDEN"` or `"LISTED"`
  (default: `"LISTED"`).

- update:

  Character. Update policy for the share; either `"AUTOMATIC"` or
  `"MANUAL"` (default: `"AUTOMATIC"`).

## Value

A message indicating that the share has been created or replaced.

## Details

This function executes a `CREATE OR REPLACE SHARE` SQL statement to
create a new share or update an existing one.

- `access` controls who can access the share.

- `visibility` controls whether the share is listed publicly or hidden.

- `update` controls whether changes to the source database are
  automatically reflected in the share. The current user is displayed
  for confirmation before execution.

## See also

Other db-manage:
[`alter_table_schema()`](https://usrbinr.github.io/md/reference/alter_table_schema.md),
[`copy_tables_to_new_location()`](https://usrbinr.github.io/md/reference/copy_tables_to_new_location.md),
[`create_database()`](https://usrbinr.github.io/md/reference/create_database.md),
[`create_if_not_exists_share()`](https://usrbinr.github.io/md/reference/create_if_not_exists_share.md),
[`create_schema()`](https://usrbinr.github.io/md/reference/create_schema.md),
[`create_table()`](https://usrbinr.github.io/md/reference/create_table.md),
[`delete_and_create_schema()`](https://usrbinr.github.io/md/reference/delete_and_create_schema.md),
[`delete_database()`](https://usrbinr.github.io/md/reference/delete_database.md),
[`delete_schema()`](https://usrbinr.github.io/md/reference/delete_schema.md),
[`delete_table()`](https://usrbinr.github.io/md/reference/delete_table.md),
[`describe_share()`](https://usrbinr.github.io/md/reference/describe_share.md),
[`drop_share()`](https://usrbinr.github.io/md/reference/drop_share.md),
[`list_owned_shares()`](https://usrbinr.github.io/md/reference/list_owned_shares.md),
[`list_shared_with_me_shares()`](https://usrbinr.github.io/md/reference/list_shared_with_me_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
create_or_replace_share(
  .con = con,
  share_name = "analytics_share",
  database_name = "sales_db",
  access = "PUBLIC",
  visibility = "LISTED",
  update = "AUTOMATIC"
)
} # }
```
