# List a MotherDuck user's instance settings

Retrieves configuration and instance-level settings for a specified
MotherDuck user, returning the results as a tidy tibble.

## Usage

``` r
list_md_user_instance(user_name, motherduck_token = "MOTHERDUCK_TOKEN")
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

- `instance_desc`: names or descriptions of instance configuration
  settings.

- `instance_values`: corresponding values for each configuration field.

## Details

This function calls the MotherDuck REST API endpoint
`https://api.motherduck.com/v1/users/{user_name}/instances` to fetch
information about the user’s active DuckDB instances and their
configuration parameters.

The current authenticated user is displayed with
[`show_current_user()`](show_current_user.md) for verification.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_tokens()`](list_md_user_tokens.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# List instance settings for a specific user
instance_tbl <- list_md_user_instance(user_name ="Bob Smith")
} # }
```
