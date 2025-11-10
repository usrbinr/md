# Create a MotherDuck database share if it does not exist

Creates a new share for a specified database in MotherDuck if it does
not already exist. Allows you to configure access, visibility, and
update settings for the share.

## Usage

``` r
create_if_not_exists_share(
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

  Character. The name of the new share to create.

- database_name:

  Character. The name of the target database to share.

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

A message confirming that the share has been created, if it did not
already exist.

## Details

This function executes a `CREATE IF NOT EXISTS` SQL statement on the
connected MotherDuck database to create a share for the specified
database.

- `access` controls who can access the share.

- `visibility` controls whether the share is listed publicly or hidden.

- `update` controls whether changes to the source database are
  automatically reflected in the share. After creation, the current user
  is displayed for confirmation.

## See also

Other db-manage:
[`alter_table_schema()`](https://usrbinr.github.io/md/reference/alter_table_schema.md),
[`copy_tables_to_new_location()`](https://usrbinr.github.io/md/reference/copy_tables_to_new_location.md),
[`create_database()`](https://usrbinr.github.io/md/reference/create_database.md),
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
[`list_shared_with_me_shares()`](https://usrbinr.github.io/md/reference/list_shared_with_me_shares.md),
[`upload_database_to_md()`](https://usrbinr.github.io/md/reference/upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
create_if_not_exists_share(
  .con = con,
  share_name = "analytics_share",
  database_name = "sales_db",
  access = "PUBLIC",
  visibility = "LISTED",
  update = "AUTOMATIC"
)
} # }
```
