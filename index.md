# Motherduck

![](reference/figures/md_logo.png)

## Overview

This is a collection of utilities to help with the management,
administration and navigation of [duckdb](https://duckdb.org/) database
either locally on your computer or in the cloud via
[motherduck](https://motherduck.com/)

Database management is incredibly easy in R with fantastic packages such
as [DBI](https://dbplyr.tidyverse.org/) and
[dbplyr](https://dbplyr.tidyverse.org/), however some databases have
specific extensions or utilities that are aren’t readily accessible via
this packages

{md} pack simplifies these common database administration task with easy
to understand syntax. {md} is built upon
[DBI](https://dbplyr.tidyverse.org/) and returns a lazy DBI object so
that you can further fully integrate your data with
[dbplyr](https://dbplyr.tidyverse.org/)

## Future ambition

Eventually, I’ll use the learning from this package to create a meta DB
utilities package so that regardless if you’re in snowflake, DuckDB,
Redshift, etc you will have generalized functions that work across your
database types

This is very much work in progress – I’ll eventually transition to the
[R7](https://rconsortium.github.io/S7/articles/S7.html) object system
but just want to get some usage first before deciding on the
architecture and structure.

Please create an issue if you have any comments or requests or reach out
if you have any feedback.

## What do I need to use this?

- [duckdb](https://duckdb.org/) R package installed on your computer
- A [motherduck](https://motherduck.com/) account
- A motherduck access token which you you can be saved to your R
  environment file with
  [`usethis::edit_r_environ()`](https://usethis.r-lib.org/reference/edit.html)

``` r
con_md <- connect_to_motherduck("MOTHERDUCK_TOKEN")
```
