# How to load data into Motherduck

## Introduction

Before we get into how to use the package, lets quickly review the three
things you will need to load data into a database:

**Database name (Catalog Name)**

- This is object that will hold your schemas, tables or views
- Different databases have their own naming convention, in motherduck,
  this is called Catalog

**Schema name**

- This is fancy name for the location that classifies and organizes your
  tables, functions, procedures, etc

**Table or view name**

- This is the name of the actual table or view that holds your data
- Table can be a physical table of data that persists whereas a view is
  a stored procedures that queries underlying tables when needed

To save or reference data, you need to either fully qualify the name
with `database_name.schema_name.table_name` or you need to be “in” your
database and schema and reference the table name.

If you uploaded data without creating a table or schema first then
duckdb will assign “temp” and “main” as the default names for your
database and schema respectively.

> **Note 1: Teminology: Duckdb vs. Motherduck**
>
> While not technically correct explanation; motherduck is a cloud based
> deployment of duckdb where you can have multiple databases, control
> access / permissions, and scale compute / storage as needed
>
> Duckdb is a essentially a single database instance in your local
> computer where you can save the database to a file or create it in
> memory.
>
> Through out these write ups, I tend to use duckdb & motherduck
> interchangeably however all functions will work motherduck database
> and most will still work with a duckdb database.
>
> If you are using a local duckdb database you can leave the database
> argument blank.

## Let’s upload some data

Later on we will show examples of how to read data from a source file,
eg. csv, parquet, or even excel directly into motherduck without loading
the data into memory but for now let’s assume you want to upload some
data that you already have loaded in your R environment.

First let us connect to our motherduck database. In order to connect,
you will need:

- motherduck account
- access token
- motherduck extension for duckdb

The [`connect_to_motherduck()`](../reference/connect_to_motherduck.md)
function will take your access token that is your environment file[^1],
install and load the extensions and then finally connect to your
motherduck instance.

> **Caution 1: connect-to-motherduck**
>
> One limitation of the connecting to motherduck from R is that you
> first need to create a local motherduck instance which then allows the
> connection to motherduck.
>
> This means you have access to both local (temporary) duckdb database
> and your cloud based motherduck databases.
>
> Check which database you are “in” with the
> [`pwd()`](../reference/pwd.md) command

``` r
1con_md <- connect_to_motherduck(motherduck_token = "MOTHERDUCK_TOKEN")
```

- 1:

  Pass your token name from your R environment file

You will get a message that prints out that actions the package took and
information about your connection

Before uploading new data, it can be helpful to check “where” you are in
your database

You can do this with the [`pwd()`](../reference/pwd.md)[^2] function
that will print out the current database & schema that you are in.

This would be the default location that you save your database unless
you clarified a different database and schema.

``` r
pwd(con_md)
```

By default, you will be in your local duckdb database even though you
are connected to motherduck

See [Caution 1](#cau-con) to understand why we start in a local database
vs. motherduck

If we want to we can also navigate to your motherduck database with the
[`cd()`](../reference/cd.md) command

``` r
cd(con_md,database = "contoso")
```

I am now in motherduck based [contoso](github.com/usrbinr/contoso)
database and any reference to schema or table would be relative to this
database.

Let’s verify that by list the all the tables in this database. We can do
that with the [`list_all_tables()`](../reference/list_all_tables.md)
function.

``` r
list_all_tables(con_md)
```

Now that we knwo how to navigate to our various databaes, lets finally
load data somee existing data into a new database and schema.
[`create_table()`](../reference/create_table.md) function will create a
new database / schema and save then load the data into a table.

``` r
1ggplot2::diamonds |>
    md::create_table(
2        .con = con_md
3        ,database_name = "vignette"
4        ,schema_name = "raw"
5        ,table_name = "diamonds"
6        ,write_type="overwrite"
        )
```

- 1:

  Pass your data into the function

- 2:

  List your motherduck connection

- 3:

  database name (either new or existing)

- 4:

  schema name (either new or existing)

- 5:

  table name

- 6:

  Either overwrite or append the data

Notice that we don’t assign this object to anything, this just silently
writes our data to our database and prints a message confirming the
performed actions as well as some summary of catalogs, schemas, tables
and shares that you haves access

To validate the data is in our database, we can do the following:

We can validate if we have successfully saved the table in our database
by running [`list_all_tables()`](../reference/list_all_tables.md).

If you want to access your motherduck data, you can simply leverage
[`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html) or
[`DBI::dbGetQuery()`](https://dbi.r-dbi.org/reference/dbGetQuery.html)
functions with your motherduck connection to pull your data.

## Organizing our data

Let’s say we want to filter and summarize this table and save it to our
database with a new table name – no problem, we can repeat the steps and
this time we will upload a DBI object instead of tibble.

``` r
1id_name <- DBI::Id("vignette","raw","diamonds")

2diamonds_summary_tbl <- dplyr::tbl(con_md,id_name) |>
    dplyr::summarise(
        .by=c(color,cut,clarity)
        ,mean_price=mean(price,na.rm=TRUE)
    )


3diamonds_summary_tbl |>
    create_table(   
    .con = con_md
    ,database_name = "vignette"
    ,schema_name = "raw"
    ,table_name = "diamonds_summary" 
    ,write_type = "overwrite"
)
```

- 1:

  You can directly call the full name or if you are already in your
  database / schema you can just call the table

- 2:

  Perform your additional cleaning, transformation, or summarization
  steps

- 3:

  Pass the DBI object to create_table and it will still save the table!

While its the same syntax,
[`create_table()`](../reference/create_table.md) will work with an R
object, duckplyr or DBI object to save that table into your database.

## Create new schema

Let’s say we want to organize these existing tables into a different
schema, we can do this by first creating a new schema and then moving
that table or alternatively loading a table directly with a new schema
and table.

``` r
    create_schema(
        .con=con_md
        ,database_name = "vignette"
        ,schema_name = "curated"
    )
```

This will create a new schema if it doesn’t exist but won’t load any
data.

``` r
list_schemas(con_md)
```

> **Default schemas**
>
> When you create a database it will have a default schema “main”, so
> even though we only created “raw” and “curated” schemas we see three
> schemas due to the default (that you can’t delete)

We can copy one of a series of tables to our new schema with
`copy_tables_to_new_location`. This accepts a tibble or DBI object of
objects with table_catalog,table_schema, table_name headers and will
copy them into your new location.

``` r
list_all_tables(con_md) |> 
  filter(
    table_catalog=="vignette"
  ) |> 
md::copy_tables_to_new_location(
    .con = con_md
    ,from_table_names = _
    ,to_database_name = "vignette"
    ,to_schema_name = "curated"
    )
```

There’s a complimentary function called `create_or_repalce_schema` which
will also create a schema the different is if there is already a schema
with that name it will delete that schema and any tables saved under it.

## Drop databaes, schemas or tables

Sometimes we need to delete databases, schemas or tables.

Be careful when you do this as its irreversible – there is no CTRL+Z to
undo this.

``` r
delete_schema(con_md,database_name = "vignette",schema_name = "curated",cascade = TRUE)
```

## How to load data directly into motherduck

For csv files we can leverage the existing duckdb function
[`duckdb::read_csv_duckdb()`](https://r.duckdb.org/reference/deprecated.html)
to directly read the a csv file or a series of csv files[^3] into your
duckdb or motherduck database

This will read the files from their source location directly into your
database without loading the files into memory which is helpful when you
are dealing with larger than memory data.

Underneath the hood the duckdb function is using the `read_csv_auto` and
you can pass the configuration options directly through the the read_csv
function if you need configuration.

``` r
write.csv(mtcars,"mtcars.csv")

# cd(schema = "raw")

duckdb::duckdb_read_csv(conn = con_md,files = "mtcars.csv",name = "mtcars")
```

For or excel, parquet or httpfs file formats, we can leverage md
read_excel_duckdb, read_parquet_duckdb() or read_httpfs_duckdb() form
the `md` package.

Similar to the `read_csv_auto` function, these leverage underlying
duckdb extensions to read these diffrent file formatas.

You can view the default configuration tables with the md::config\_\*
family of tables

``` r
1openxlsx::write.xlsx(starwars,"starwars.xlsx")


read_excel(
2    .con=con_md
3    ,to_database_name = "vignette"
4    ,to_schema_name = "main"
5    ,to_table_name = "starwars"
6    ,file_path = "starwars.xlsx"
7    ,header = TRUE
8    ,sheet = "Sheet 1"
9    ,all_varchar  = TRUE
10    ,write_type = "overwrite"
)
```

- 1:

  Create a excel file

- 2:

  Pass through our connection

- 3:

  Select the database we want the table to be saved in

- 4:

  Select the schema we want the table to be saved in

- 5:

  Select the table name

- 6:

  Select the filepath to the excel file  

- 7:

  Clarify if we want the first line to be used as headers

- 8:

  Clarify the sheet name to read

- 9:

  Clarify if all columns types be read in as characters

- 10:

  Select if we should overwrite to an existing table or append

Below are the list of configuration options available to be passed
through to respective read\_\* functions.

> **Configuration options**
>
> - [CSV](#tabset-1-1)
> - [Excel](#tabset-1-2)
> - [Parquet](#tabset-1-3)
>
> &nbsp;
>
> - 

[^1]: Use
    [`usethis::edit_r_environ()`](https://usethis.r-lib.org/reference/edit.html)
    to save your access token to a variable name

[^2]: Naming convention is inspired by linux commands

[^3]: as long as they have the same header structure
