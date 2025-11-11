# How to connect to motherduck

## Lets see the package in action

### Create a duckdb instance and Connect to your motherduck account

When creating a duckdb database, you have three options

1.  A in-memory based instance that exists in your local computer
2.  A file based instance that exists in your local computer
3.  A cloud-based instance through [motherduck](https://motherduck.com/)

To create options 1 or 2 you can simply use either the
[duckdb](https://duckdb.org/) or the
[duckplyr](https://duckplyr.tidyverse.org/index.html) packages.

To use option 3, you will need to create a motherudck account and
generate an access token. Once created, save your access token to an
your R enviorment with `usethis::edit_r_environ()`. I recommend using
`MOTHERDUCK_TOKEN` as your variable name.

Once completed, you can simply use the
[`connect_to_motherduck()`](https://usrbinr.github.io/md/reference/connect_to_motherduck.md)
function and pass through your token variable name and optional
configuration options.

> **Whats the difference between Motherduck and Duckdb?**
>
> - Duckdb is a database that you can deploy and run either temporary or
>   permanently in your computer. If you run it via your local computer,
>   it is only available on your computer
>
> - Motherduck is a cloud based deployment of duckdb which means you can
>   save your data in the cloud or access it locally
>
> - Most core functions in this package work for both motherduck or
>   duckdb database
>
> - It is more of a question if you want you data to be access only
>   locally on your computer or if you want to be able to access it
>   remotely via the cloud

``` r
library(md)


con_md <- md::connect_to_motherduck("MOTHERDUCK_TOKEN")
```

This will return a connection and print statement indicating if
connection status.

At any time you can validate your connection status with
[`validate_md_connection_status()`](https://usrbinr.github.io/md/reference/validate_md_connection_status.md)

``` r
library(md)

validate_md_connection_status(con_md)
```

> **how to create a motherduck account and access token?**
>
> 1.  Go to [motherduck](https://motherduck.com/) and create an account,
>     free options are available
> 2.  Go to your user name in the top right, click settings then click
>     access tokens
> 3.  Click create token and then name your token and copy the token
>     code
> 4.  You will need this token to access your account
> 5.  If you want to access it via R then simplest way is to save your
>     access code as a variable in your r environment
> 6.  Simply leverage the [usethis](https://usethis.r-lib.org/) function
>     `edit_r_environ()` to set your access code to a variable and save
>     it – this is one time activity
> 7.  To check if your correctly saved your variable then you can use
>     the Sys.getenv(“var_name”) with “var_name” the named you assigned
>     your access token to
> 8.  Going forward, if you want to access your token you don’t need to
>     re-type the access token, simply remember your variable name
>
> - First you will need a motherduck account, which has both free and
>   paid tiers
>
> - Once you’ve created an account, simply, go to your settings and
>   click ‘Access Tokens’ under your ‘Integrations’
>
> - Keep this secure and safe as this lets you connect to your online
>   database to read or write data
>
> - Open R and use the `usethis::edit_r_environ()` function to put your
>   motherduck token as a variable in your enviornment profile
>
>   - MOTHERDUCK_TOKEN=‘tokenID’
>
> - From there you can use the
>   `connect_to_motherduck("MOTHERDUCK_TOKEN")`
>
> - This will use the [DBI](https://dbi.r-dbi.org/) library to create a
>   connection to your mother duck instance

When connecting to motherduck there are a number of configuration
options available, you can reference them via the `md::db_config` which
will pull a list of options and their default values

To change these, simply edit the configuration options you want and then
pass the list as an argument
[`connect_to_motherduck()`](https://usrbinr.github.io/md/reference/connect_to_motherduck.md)
or [`duckdb()`](https://r.duckdb.org/reference/duckdb.html) if
connecting locally

You can see the full list of duckdb configuration options
[here](https://duckdb.org/docs/stable/configuration/overview.html) or
alternatively you can use `list_settings()` to see your current
configuration options.

``` r
1config <- md::db_config


2config$allow_community_extensions <- "true"

3con_md <- connect_to_motherduck("MOTHERDUCK_TOKEN",config = config)
```

- 1:

  Access the default configuration options via the built in list

- 2:

  change the options you want, ensure you leverage duckdb syntax

- 3:

  pass the modtified list to config option in the function

At any time you can see what configuration arguments are for your
connection with `md::list_settings()`.

``` r
list_setting(con_md)
```

Congratulations, you’ve set connected to your motherduck database from
R!

If you’re new to databases, it will be helpful to have a basic
understanding of database management - don’t worry the basics are
straight forward and won’t overwhelm you, below are a list of resources
I found helpful.

- [Posit solutions guide to working with
  databases](https://solutions.posit.co/connections/db/)
- [dbplyr package
  page](https://dbplyr.tidyverse.org/articles/dbplyr.html)
- [Motherduck how to
  guide](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/)
- [duckdb documentation
  guide](https://duckdb.org/docs/stable/sql/introduction)

Please see the {md} package down website to see additional documentation
on how to use the functions and motherduck
