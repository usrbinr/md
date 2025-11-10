# Drop a MotherDuck share

Drops (deletes) a specified share from your MotherDuck account. If the
share does not exist, a warning is displayed. This function safely
validates the connection and share name before executing the operation.

## Usage

``` r
drop_share(.con, share_name)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- share_name:

  Character. The name of the share to be dropped.

## Value

Invisibly returns `NULL`. Side effect: the specified share is removed if
it exists.

## Details

The function first validates that the connection is active. It then
checks whether the specified share exists in the account. If it does,
the share is dropped using a `DROP SHARE IF EXISTS` SQL command. If the
share does not exist, a warning is shown. After the operation, the
current user is displayed.

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
[`list_owned_shares()`](list_owned_shares.md),
[`list_shared_with_me_shares()`](list_shared_with_me_shares.md),
[`upload_database_to_md()`](upload_database_to_md.md)

## Examples

``` r
if (FALSE) { # \dontrun{
drop_share(con_md, "test_share")
} # }
```
