# User Information Report

This function generates a report that shows the current user and their
assigned role within the database. It queries the database to retrieve
the current user using `current_user()` and the current role using
`current_role()`. The output is displayed in a clear and formatted
manner, with the user name and role listed in an unordered list.

## Usage

``` r
cli_show_user(.con)
```

## Arguments

- .con:

  A valid `DBI` connection (DuckDB / MotherDuck).

## Value

This function doesn't return any value. It generates a formatted user
report with the current user's name and role as output.
