# Create a MotherDuck access token

Creates a new access token for a specified MotherDuck user using the
REST API. Tokens can be configured with a specific type, name, and
expiration time.

## Usage

``` r
create_md_access_token(
  user_name,
  token_type,
  token_name,
  token_expiration_number,
  token_expiration_unit,
  motherduck_token = "MOTHERDUCK_TOKEN"
)
```

## Arguments

- user_name:

  A character string specifying the MotherDuck user name whose tokens
  should be listed.

- token_type:

  Character. The type of token to create. Must be one of: `"read_write"`
  or `"read_scaling"`.

- token_name:

  Character. A descriptive name for the token.

- token_expiration_number:

  Numeric. The duration of the token’s validity, in the units specified
  by `token_expiration_unit`. Minimum value is 300 seconds.

- token_expiration_unit:

  Character. The unit of time for the token expiration. One of
  `"seconds"`, `"minutes"`, `"days"`, `"weeks"`, `"months"`, `"years"`,
  or `"never"`.

- motherduck_token:

  Character. Either the name of an environment variable containing your
  MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token
  itself.

## Value

A tibble containing the API response, including the username and the
token attributes.

## Details

This function calls the MotherDuck REST API endpoint
`https://api.motherduck.com/v1/users/{user_name}/tokens` to create a new
token for the specified user. The token’s time-to-live (TTL) is
calculated in seconds from `token_expiration_number` and
`token_expiration_unit`. The authenticated user must have administrative
privileges to create tokens.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`list_md_user_tokens()`](list_md_user_tokens.md),
[`show_current_user()`](show_current_user.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Create a temporary read/write token for user "alejandro_hagan" valid for 1 hour
create_md_access_token(
  user_name = "alejandro_hagan",
  token_type = "read_write",
  token_name = "temp_token",
  token_expiration_number = 1,
  token_expiration_unit = "hours",
  motherduck_token = "MOTHERDUCK_TOKEN"
)
} # }
```
