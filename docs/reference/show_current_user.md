# Show current database user

Return or print the current database user for a MotherDuck / DuckDB
connection.

## Usage

``` r
show_current_user(.con, motherduck_token, return = "msg")
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

- motherduck_token:

  Character. Either the name of an environment variable containing your
  MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token
  itself.

- return:

  Character scalar, one of `"msg"` or `"arg"`. Default: `"msg"`.

## Details

This helper queries the active DB connection for the current user (via
`SELECT current_user`). You may either provide an existing DBI
connection via `.con` or provide a `motherduck_token` and let the
function open a short-lived connection for you. When the function opens
a connection it will close it before returning.

The function supports two output modes:

- `"msg"` — prints a small informative message and returns the result
  invisibly (useful for interactive use),

- `"arg"` — returns a tibble containing the `current_user` column.

## See also

Other db-api:
[`configure_md_user_settings()`](configure_md_user_settings.md),
[`create_md_access_token()`](create_md_access_token.md),
[`create_md_user()`](create_md_user.md),
[`delete_md_access_token()`](delete_md_access_token.md),
[`delete_md_user()`](delete_md_user.md),
[`list_md_active_accounts()`](list_md_active_accounts.md),
[`list_md_user_instance()`](list_md_user_instance.md),
[`list_md_user_tokens()`](list_md_user_tokens.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Using an existing connection
con <- connect_to_motherduck("my_token")
show_current_user(.con = con, return = "msg")

# Let the function open a connection from a token
tbl <- show_current_user(motherduck_token = "my_token", return = "arg")
} # }
```
