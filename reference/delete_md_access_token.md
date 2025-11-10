# Delete a MotherDuck user's access token

Deletes a specific access token for a given MotherDuck user using the
REST API. This operation requires administrative privileges and a valid
API token.

## Usage

``` r
delete_md_access_token(
  user_name,
  token_name,
  motherduck_token = "MOTHERDUCK_TOKEN"
)
```

## Arguments

- user_name:

  A character string specifying the MotherDuck user name whose tokens
  should be listed.

- token_name:

  Character. The name of the access token to delete.

- motherduck_token:

  Character. Either the name of an environment variable containing your
  MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token
  itself.

## Value

A tibble summarizing the API response, typically including the username
and deletion status of the token.

## Details

This function calls the MotherDuck REST API endpoint
`https://api.motherduck.com/v1/users/{user_name}/tokens/{token_name}`
using a `DELETE` request to remove the specified token. The
authenticated user must have sufficient permissions to perform token
management.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`list_md_user_tokens()`](list_md_user_tokens.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Delete a token named "temp_token" for user "alejandro_hagan"
delete_md_access_token(
  user_name = "alejandro_hagan",
  token_name = "temp_token",
  motherduck_token = "MOTHERDUCK_TOKEN"
)
} # }
```
