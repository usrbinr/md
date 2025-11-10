# Delete a MotherDuck user

Sends a `DELETE` request to the MotherDuck REST API to permanently
remove a user from your organization. This operation requires
administrative privileges and a valid MotherDuck access token.

## Usage

``` r
delete_md_user(user_name, motherduck_token = "MOTHERDUCK_TOKEN")
```

## Arguments

- user_name:

  A character string specifying the MotherDuck user name whose tokens
  should be listed.

- motherduck_token:

  Character. Either the name of an environment variable containing your
  MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token
  itself.

## Value

A tibble summarizing the API response, including the username and
deletion status.

## Details

This function calls the [MotherDuck Users
API](https://motherduck.com/docs/sql-reference/rest-api/users-delete/)
endpoint to delete the specified user. The authenticated user
(associated with the provided token) must have sufficient permissions to
perform user management actions.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`list_md_user_tokens()`](list_md_user_tokens.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Delete a user named "bob_smith" using an admin token stored in an environment variable
delete_md_user("bob_smith", "MOTHERDUCK_TOKEN")
} # }
```
