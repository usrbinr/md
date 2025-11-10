# List active MotherDuck accounts

Retrieves a list of active MotherDuck accounts available to the
authenticated user, returning the results as a tidy tibble.

## Usage

``` r
list_md_active_accounts(motherduck_token = "MOTHERDUCK_TOKEN")
```

## Arguments

- motherduck_token:

  Character. Either the name of an environment variable containing your
  MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token
  itself.

## Value

A tibble with two columns:

- `account_settings`: configuration keys for the active accounts.

- `account_values`: corresponding configuration values.

## Details

This function queries the MotherDuck REST API endpoint
(`https://api.motherduck.com/v1/active_accounts`) using the provided or
environment-resolved authentication token. The current user name is also
displayed via [`show_current_user()`](show_current_user.md).

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`list_md_user_tokens()`](list_md_user_tokens.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Retrieve active accounts for the authenticated user
accounts_tbl <- list_md_active_accounts()
print(accounts_tbl)
} # }
```
