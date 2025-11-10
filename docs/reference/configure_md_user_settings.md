# Configure a MotherDuck user's settings

Updates a MotherDuck user's configuration settings, including token
type, instance size, and flock size. This function uses the MotherDuck
REST API to apply the changes for the specified user.

## Usage

``` r
configure_md_user_settings(
  user_name,
  motherduck_token = "MOTHERDUCK_TOKEN",
  token_type = "read_write",
  instance_size = "pulse",
  flock_size = 0
)
```

## Arguments

- user_name:

  Character. The username of the MotherDuck user to configure.

- motherduck_token:

  Character. The admin user's MotherDuck token or environment variable
  name (default: `"MOTHERDUCK_TOKEN"`).

- token_type:

  Character. The type of access token for the user; must be
  `"read_write"` or `"read_scaling"` (default: `"read_write"`).

- instance_size:

  Character. The instance size for the user; must be one of `"pulse"`,
  `"standard"`, `"jumbo"`, `"mega"`, `"giga"` (default: `"pulse"`).

- flock_size:

  Numeric. The flock size for the user; must be a whole number between 0
  and 60 (default: 0).

## Value

A tibble containing the API response, including the updated settings for
the user.

## Details

This function validates each parameter before making a `PUT` request to
the MotherDuck API. It ensures that:

- `token_type` is valid using `validate_token_type()`.

- `instance_size` is valid using `validate_instance_size()`.

- `flock_size` is a valid integer using `validate_flock_size()`. The API
  response is returned as a tibble for easy inspection.

## See also

Other db-api: [`create_md_access_token()`](create_md_access_token.md),
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
configure_md_user_settings(
  user_name = "alice",
  motherduck_token = "MOTHERDUCK_TOKEN",
  token_type = "read_write",
  instance_size = "pulse",
  flock_size = 10
)
} # }
```
