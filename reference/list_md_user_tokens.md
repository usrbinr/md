# List a MotherDuck user's tokens

Retrieves all active authentication tokens associated with a specific
MotherDuck user account, returning them as a tidy tibble.

## Usage

``` r
list_md_user_tokens(user_name, motherduck_token = "MOTHERDUCK_TOKEN")
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

A tibble with two columns:

- `token_settings`: metadata fields associated with each token.

- `token_values`: corresponding values for those fields.

## Details

This function queries the MotherDuck REST API endpoint
`https://api.motherduck.com/v1/users/{user_name}/tokens` to list the
tokens available for the specified user.

It uses the provided or environment-resolved `motherduck_token` for
authorization. If `motherduck_token` is not explicitly provided, the
function attempts to resolve it from the `MOTHERDUCK_TOKEN` environment
variable The current authenticated user is displayed via
[`show_current_user()`](show_current_user.md) for verification.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# List tokens for a specific user
tokens_tbl <- list_md_user_tokens(user_name = "alejandro_hagan")
print(tokens_tbl)
} # }
```
