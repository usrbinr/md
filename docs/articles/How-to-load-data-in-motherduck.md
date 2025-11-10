# How to load data into motherduck

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

The
[`connect_to_motherduck()`](https://usrbinr.github.io/md/reference/connect_to_motherduck.md)
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
> [`pwd()`](https://usrbinr.github.io/md/reference/pwd.md) command

``` r
1con_md <- connect_to_motherduck(motherduck_token = "MOTHERDUCK_TOKEN")
```

- 1:

  Pass your token name from your R environment file

``` fansi
── Extension Load & Install Report ─────────────────────────────────────────────
```

``` fansi
Installed and loaded 1 extension: motherduck
```

    Use `list_extensions()` to list extensions, status and their descriptions

``` fansi
Use `install_extensions()` to install new duckdb extensions
```

``` fansi
See <https://duckdb.org/docs/stable/extensions/overview.html> for more
information
```

``` fansi
── Connection Status Report: ──
```

``` fansi
✔ You are connected to MotherDuck
```

You will get a message that prints out that actions the package took and
information about your connection

Before uploading new data, it can be helpful to check “where” you are in
your database

You can do this with the
[`pwd()`](https://usrbinr.github.io/md/reference/pwd.md)[^2] function
that will print out the current database & schema that you are in.

This would be the default location that you save your database unless
you clarified a different database and schema.

``` r
pwd(con_md)
```

    → Current role: `duckdb`

``` fansi
# A tibble: 1 × 2
  current_database  current_schema
  <chr>             <chr>
1 file5db7f78aab7b7 main
```

By default, you will be in your local duckdb database even though you
are connected to motherduck

See [Caution 1](#cau-con) to understand why we start in a local database
vs. motherduck

If we want to we can also navigate to your motherduck database with the
[`cd()`](https://usrbinr.github.io/md/reference/cd.md) command

``` r
cd(con_md,database = "contoso")
```

I am now in motherduck based
[contoso](https://usrbinr.github.io/md/articles/github.com/usrbinr/contoso)
database and any reference to schema or table would be relative to this
database.

Let’s verify that by list the all the tables in this database. We can do
that with the
[`list_all_tables()`](https://usrbinr.github.io/md/reference/list_all_tables.md)
function.

``` r
list_all_tables(con_md)
```

Now that we knwo how to navigate to our various databaes, lets finally
load data somee existing data into a new database and schema.
[`create_table()`](https://usrbinr.github.io/md/reference/create_table.md)
function will create a new database / schema and save then load the data
into a table.

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

``` fansi
── Status: ─────────────────────────────────────────────────────────────────────
```

``` fansi
── Connection Status Report: ──
```

``` fansi
✔ You are connected to MotherDuck
```

``` fansi
── User Report: ──
```

``` fansi
• User Name: "alejandro_hagan"
```

``` fansi
• Role: "duckdb"
```

``` fansi
── Catalog Report: ──
```

``` fansi
• Current Database: "vignette"
```

``` fansi
• Current Schema: "raw"
```

``` fansi
• # Total Catalogs you have access to: 13
```

``` fansi
• # Total Tables you have access to: 60
```

``` fansi
• # Total Shares you have access to: 2
```

``` fansi
• # Tables in this catalog you have access to: 4
```

``` fansi
• # Tables in this catalog & schema you have access to: 1
```

``` fansi
── Action Report: ──
```

``` fansi
✔ Inserted into existing database "vignette"
```

``` fansi
✔ Using existing schema "raw"
```

Notice that we don’t assign this object to anything, this just silently
writes our data to our database and prints a message confirming the
performed actions as well as some summary of catalogs, schemas, tables
and shares that you haves access

To validate the data is in our database, we can do the following:

We can validate if we have successfully saved the table in our database
by running
[`list_all_tables()`](https://usrbinr.github.io/md/reference/list_all_tables.md).

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
[`create_table()`](https://usrbinr.github.io/md/reference/create_table.md)
will work with an R object, duckplyr or DBI object to save that table
into your database.

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

``` fansi
# Source:   SQL [?? x 2]
# Database: DuckDB 1.4.1 [hagan@Linux 6.16.3-76061603-generic:R 4.5.1//tmp/Rtmp2yTjzC/file5db7f78aab7b7.duckdb]
  catalog_name schema_name
  <chr>        <chr>
1 vignette     raw
2 vignette     main
3 vignette     test
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
> - | name | description | type | default |
>   |----|----|----|----|
>   | all_varchar | Skip type detection and assume all columns are of type VARCHAR. This option is only supported by the read_csv function. | BOOL | false |
>   | allow_quoted_nulls | Allow the conversion of quoted values to NULL values | BOOL | true |
>   | auto_detect | Auto detect CSV parameters. | BOOL | true |
>   | auto_type_candidates | Types that the sniffer uses when detecting column types. The VARCHAR type is always included as a fallback option. See example. | TYPE\[\] | default types |
>   | buffer_size | Size of the buffers used to read files, in bytes. Must be large enough to hold four lines and can significantly impact performance. | BIGINT | 16 \* max_line_size |
>   | columns | Column names and types, as a struct (e.g., {'col1': 'INTEGER', 'col2': 'VARCHAR'}). Using this option disables auto detection of the schema. | STRUCT | (empty) |
>   | comment | Character used to initiate comments. Lines starting with a comment character (optionally preceded by space characters) are completely ignored; other lines containing a comment character are parsed only up to that point. | VARCHAR | (empty) |
>   | compression | Method used to compress CSV files. By default this is detected automatically from the file extension (e.g., t.csv.gz will use gzip, t.csv will use none). Options are none, gzip, zstd. | VARCHAR | auto |
>   | dateformat | Date format used when parsing and writing dates. | VARCHAR | (empty) |
>   | date_format | Alias for dateformat; only available in the COPY statement. | VARCHAR | (empty) |
>   | decimal_separator | Decimal separator for numbers. | VARCHAR | . |
>   | delim | Delimiter character used to separate columns within each line, e.g., , ; \t. The delimiter character can be up to 4 bytes, e.g., 🦆. Alias for sep. | VARCHAR | , |
>   | delimiter | Alias for delim; only available in the COPY statement. | VARCHAR | , |
>   | escape | String used to escape the quote character within quoted values. | VARCHAR | " |
>   | encoding | Encoding used by the CSV file. Options are utf-8, utf-16, latin-1. Not available in the COPY statement (which always uses utf-8). | VARCHAR | utf-8 |
>   | filename | Add path of the containing file to each row, as a string column named filename. Relative or absolute paths are returned depending on the path or glob pattern provided to read_csv, not just filenames. Since DuckDB v1.3.0, the filename column is added automatically as a virtual column and this option is only kept for compatibility reasons. | BOOL | false |
>   | force_not_null | Do not match values in the specified columns against the NULL string. In the default case where the NULL string is empty, this means that empty values are read as zero-length strings instead of NULLs. | VARCHAR\[\] | \[\] |
>   | header | First line of each file contains the column names. | BOOL | false |
>   | hive_partitioning | Interpret the path as a Hive partitioned path. | BOOL | (auto-detected) |
>   | ignore_errors | Ignore any parsing errors encountered. | BOOL | false |
>   | max_line_size or maximum_line_size | Maximum line size, in bytes. Not available in the COPY statement. | BIGINT | 2000000 |
>   | names or column_names | Column names, as a list. See example. | VARCHAR\[\] | (empty) |
>   | new_line | New line character(s). Options are '\r','\n', or '\r\n'. The CSV parser only distinguishes between single-character and double-character line delimiters. Therefore, it does not differentiate between '\r' and '\n'. | VARCHAR | (empty) |
>   | normalize_names | Normalize column names. This removes any non-alphanumeric characters from them. Column names that are reserved SQL keywords are prefixed with an underscore character (\_). | BOOL | false |
>   | null_padding | Pad the remaining columns on the right with NULL values when a line lacks columns. | BOOL | false |
>   | nullstr or null | Strings that represent a NULL value. | VARCHAR or VARCHAR\[\] | (empty) |
>   | parallel | Use the parallel CSV reader. | BOOL | true |
>   | quote | String used to quote values. | VARCHAR | " |
>   | rejects_scan | Name of the temporary table where information on faulty scans is stored. | VARCHAR | reject_scans |
>   | rejects_table | Name of the temporary table where information on faulty lines is stored. | VARCHAR | reject_errors |
>   | rejects_limit | Upper limit on the number of faulty lines per file that are recorded in the rejects table. Setting this to 0 means that no limit is applied. | BIGINT | 0 |
>   | sample_size | Number of sample lines for auto detection of parameters. | BIGINT | 20480 |
>   | sep | Delimiter character used to separate columns within each line, e.g., , ; \t. The delimiter character can be up to 4 bytes, e.g., 🦆. Alias for delim. | VARCHAR | , |
>   | skip | Number of lines to skip at the start of each file. | BIGINT | 0 |
>   | store_rejects | Skip any lines with errors and store them in the rejects table. | BOOL | false |
>   | strict_mode | Enforces the strictness level of the CSV Reader. When set to true, the parser will throw an error upon encountering any issues. When set to false, the parser will attempt to read structurally incorrect files. It is important to note that reading structurally incorrect files can cause ambiguity; therefore, this option should be used with caution. | BOOL | true |
>   | thousands | Character used to identify thousands separators in numeric values. It must be a single character and different from the decimal_separator option. | VARCHAR | (empty) |
>   | timestampformat | Timestamp format used when parsing and writing timestamps. | VARCHAR | (empty) |
>   | timestamp_format | Alias for timestampformat; only available in the COPY statement. | VARCHAR | (empty) |
>   | types or dtypes or column_types | Column types, as either a list (by position) or a struct (by name). See example. | VARCHAR\[\] or STRUCT | (empty) |
>   | union_by_name | Align columns from different files by column name instead of position. Using this option increases memory consumption. | BOOL | false |
>
> | Option | Type | Default | Description |
> |----|----|----|----|
> | header | BOOLEAN | automatically inferred | Whether to treat the first row as containing the names of the resulting columns. |
> | sheet | VARCHAR | automatically inferred | The name of the sheet in the xlsx file to read. Default is the first sheet. |
> | all_varchar | BOOLEAN | false | Whether to read all cells as containing VARCHARs. |
> | ignore_errors | BOOLEAN | false | Whether to ignore errors and silently replace cells that cant be cast to the corresponding inferred column type with NULL's. |
> | range | VARCHAR | automatically inferred | The range of cells to read, in spreadsheet notation. For example, A1:B2 reads the cells from A1 to B2. If not specified the resulting range will be inferred as rectangular region of cells between the first row of consecutive non-empty cells and the first empty row spanning the same columns. |
> | stop_at_empty | BOOLEAN | automatically inferred | Whether to stop reading the file when an empty row is encountered. If an explicit range option is provided, this is false by default, otherwise true. |
> | empty_as_varchar | BOOLEAN | false | Whether to treat empty cells as VARCHAR instead of DOUBLE when trying to automatically infer column types. |
>
> | Name | Description | Type | Default |
> |----|----|----|----|
> | binary_as_string | Parquet files generated by legacy writers do not correctly set the UTF8 flag for strings, causing string columns to be loaded as BLOB instead. Set this to true to load binary columns as strings. | BOOL | false |
> | encryption_config | Configuration for Parquet encryption. | STRUCT | \- |
> | filename | Whether or not an extra filename column should be included in the result. Since DuckDB v1.3.0, the filename column is added automatically as a virtual column and this option is only kept for compatibility reasons. | BOOL | false |
> | file_row_number | Whether or not to include the file_row_number column. | BOOL | false |
> | hive_partitioning | Whether or not to interpret the path as a Hive partitioned path. | BOOL | (auto-detected) |
> | union_by_name | Whether the columns of multiple schemas should be unified by name, rather than by position. | BOOL | false |

[^1]: Use
    [`usethis::edit_r_environ()`](https://usethis.r-lib.org/reference/edit.html)
    to save your access token to a variable name

[^2]: Naming convention is inspired by linux commands

[^3]: as long as they have the same header structure
