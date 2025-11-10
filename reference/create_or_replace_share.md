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

Other db-manage: [`alter_table_schema()`](alter_table_schema.md),
[`copy_tables_to_new_location()`](copy_tables_to_new_location.md),
[`create_database()`](create_database.md),
[`create_if_not_exists_share()`](create_if_not_exists_share.md),
[`create_schema()`](create_schema.md),
[`create_table()`](create_table.md),
[`delete_and_create_schema()`](delete_and_create_schema.md),
[`delete_database()`](delete_database.md),
[`delete_schema()`](delete_schema.md),
[`delete_table()`](delete_table.md),
[`describe_share()`](describe_share.md),
[`drop_share()`](drop_share.md),
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)

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
