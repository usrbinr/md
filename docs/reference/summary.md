# Summarize a Lazy DBI Table

The `summary.tbl_lazy()` method provides a database-aware summary
interface for lazy tables created via **dbplyr**. Instead of collecting
data into R, it constructs a SQL `SUMMARIZE` query and executes it
remotely, returning another lazy table reference.

## Usage

``` r
# S3 method for class 'tbl_lazy'
summary(object, ...)
```

## Arguments

- object:

  A [`tbl_lazy`](https://dbplyr.tidyverse.org/reference/tbl_lazy.html)
  object representing a remote database table or query.

- ...:

  Additional arguments (currently unused). Present for S3 method
  compatibility.

## Value

A [`tbl_lazy`](https://dbplyr.tidyverse.org/reference/tbl_lazy.html)
object containing the summarized results, still backed by the remote
database connection.

## Details

This method does **not** pull data into memory. Instead, it creates a
new lazy query object representing the database-side summary. To
retrieve the summarized data, use
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html) on the
returned object.

## Examples

``` r
if (FALSE) { # \dontrun{
library(DBI)
library(duckdb)
library(dplyr)

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
dbWriteTable(con, "mtcars", mtcars)

tbl_obj <- tbl(con, "mtcars")

# Returns a lazy summary table
summary(tbl_obj)

dbDisconnect(con)
} # }

```
