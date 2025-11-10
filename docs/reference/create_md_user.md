# Create a new MotherDuck user

Sends a `POST` request to the MotherDuck REST API to create a new user
within your organization. This operation requires administrative
privileges and a valid access token.

## Usage

``` r
create_md_user(user_name, motherduck_token = "MOTHERDUCK_TOKEN")
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

A tibble summarizing the API response, typically containing the newly
created username and associated metadata.

## Details

This function calls the [MotherDuck Users
API](https://motherduck.com/docs/sql-reference/rest-api/users-create/)
endpoint to create a new user under the authenticated account. The
provided token must belong to a user with permissions to manage
organization-level accounts.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`list_md_user_tokens()`](list_md_user_tokens.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Create a new user in MotherDuck using an admin token stored in an environment variable
create_md_user("test_20250913", "MOTHERDUCK_TOKEN")
} # }
```
