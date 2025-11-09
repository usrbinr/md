

## cd()---------------

describe("cd()", {

  it("should create a schema and switch to it successfully", {
    # Connect to in-memory DuckDB
    con_db <- DBI::dbConnect(duckdb::duckdb())

    # Create schema
    create_schema(con_db, schema_name = "test_schema")

    # Check current working directory / schema before switching
    pwd(con_db)

    test_schema <- "test_schema"

    # Change to the new schema
    cd(con_db, database_name = "memory", schema_name = test_schema)

    # Get current schema
    current_schema <- pwd(con_db) |> dplyr::pull(current_schema)

    # Expect the current schema to match the one we created
    expect_equal(current_schema, test_schema)

    # Clean up
    DBI::dbDisconnect(con_db, shutdown = TRUE)
  })

  it("errors on invalid database", {
    con_db <- DBI::dbConnect(duckdb::duckdb())
    expect_error(cd(con_db, database_name = "invalid_db"))
    DBI::dbDisconnect(con_db)
  })


  it("errors on invalid schema", {
    con_db <- DBI::dbConnect(duckdb::duckdb())
    expect_error(cd(
      con_db,
      database_name = "main",
      schema_name = "invalid_schema"
    ))
    DBI::dbDisconnect(con_db)
  })
})

## pwd()----------------

describe("pwd()", {

  it("should retun the right class, column names and row", {
    con_db <- DBI::dbConnect(duckdb::duckdb())

    out <- pwd(con_db)

    testthat::expect_s3_class(out, "tbl_df")
    testthat::expect_true(all(
      c("current_database", "current_schema") %in% names(out)
    ))
    testthat::expect_equal(nrow(out), 1)
    DBI::dbDisconnect(con_db)
  })


  testthat::it("returns the expected role value", {
    con_db <- DBI::dbConnect(duckdb::duckdb())
    out <- pwd(con_db)
    role <- DBI::dbGetQuery(con_db, "select current_role();")[1, 1]
    testthat::expect_equal(
      role,
      'duckdb',
      info = "The current role should match the 'duckdb'"
    )
    DBI::dbDisconnect(con_db)

  })

})












## create-table-tbl---------



describe("create_table_tbl()", {

  it("creates (overwrite) a table and adds audit fields", {
    # ephemeral connection for this test
    con_db <- DBI::dbConnect(duckdb::duckdb())

    # simple data to write
    df <- tibble::tibble(id = 1:3, value = c("a", "b", "c"))

    # call function (explicit database/schema to avoid branching)
    create_table(.data = df,
                     .con = con_db,
                     database_name = "memory",
                     schema_name = "main",
                     table_name = "test_table_overwrite",
                     write_type = "overwrite")

    # expectations: table exists
    expect_true(DBI::dbExistsTable(con_db, "test_table_overwrite"))

    # read back and check audit fields & row count
    out <- DBI::dbReadTable(con_db, "test_table_overwrite")
    expect_equal(nrow(out), nrow(df))
    expect_true("upload_date" %in% names(out))
    expect_true("upload_time" %in% names(out))

    # upload_date should be a Date (when read from DB it may be character depending on backend;
    # try coercion and ensure values are present and parseable)
    expect_true(!all(is.na(as.Date(out$upload_date))))

    DBI::dbDisconnect(con_db)
  })


  it("appends rows when write_type = 'append'", {
    con_db <- DBI::dbConnect(duckdb::duckdb())


    df1 <- tibble::tibble(id = 1:2, value = c("x", "y"))
    df2 <- tibble::tibble(id = 3:4, value = c("z", "w"))

    # initial write (overwrite)
    create_table(.data = df1,
                     .con = con_db,
                     database_name = "memory",
                     schema_name = "main",
                     table_name = "test_table_append",
                     write_type = "overwrite")

    # append data
    create_table_tbl(.data = df2,
                     .con = con_db,
                     database_name = "memory",
                     schema_name = "main",
                     table_name = "test_table_append",
                     write_type = "append")

    out <- DBI::dbReadTable(con_db, "test_table_append")
    # Expect total rows equal to sum of two data frames
    expect_equal(nrow(out), nrow(df1) + nrow(df2))

    # ensure ids 1:4 present
    expect_true(all(1:4 %in% out$id))
  })

  it("writes upload_time values that look like times", {


    con_db <- DBI::dbConnect(duckdb::duckdb())

    df <- tibble::tibble(a = 1L)

    create_table(.data = df,
                     .con = con_db,
                     database_name = "memory",
                     schema_name = "main",
                     table_name = "tbl_time_check")

    out <- DBI::dbReadTable(con_db, "tbl_time_check")
    # upload_time could be character; check it contains a colon (HH:MM:SS)
    expect_true(any(grepl(":", out$upload_time)))

    DBI::dbDisconnect(con_db)

  })

})



## create_tbl_dbi---------

describe("create_table_dbi", {


  con_db <- DBI::dbConnect(duckdb::duckdb())

  # create input data
  df <- tibble::tibble(id = 1:3, val = c("a","b","c"))

  df2 <- tibble::tibble(id = 4:6, val = c("d","e","f"))

  DBI::dbWriteTable(con_db,name = "dbi_tbl",value = df)

  DBI::dbWriteTable(con_db,name = "dbi_tbl2",value = df2)

  out <- dplyr::tbl(con_db, "dbi_tbl") |>
    dplyr::mutate(
      new_col=1
    )

  out2 <- dplyr::tbl(con_db, "dbi_tbl2") |>
    dplyr::mutate(
      new_col=2
    )

  create_table(.data = out,
               .con = con_db,
               database_name = "memory",
               schema_name = "main",
               table_name = "dbi_overwrite",
               write_type = "overwrite")


  create_table(.data = out2,
               .con = con_db,
               database_name = "memory",
               schema_name = "main",
               table_name = "dbi_overwrite",
               write_type = "append")


  it("overwrite: sucessfully creates ", {

    expect_true(DBI::dbExistsTable(con_db, "dbi_overwrite"))

  })

  it("returns all id columns included time and new column", {

    expect_true(all(colnames(DBI::dbReadTable(con_db,"dbi_overwrite")) %in% c("new_col","id","val","upload_date","upload_time")))

  })

  it("append is successful and returns 6 rows for two 3 rows tables", {

    expect_equal(nrow(DBI::dbReadTable(con_db,"dbi_overwrite")),nrow(df)+nrow(df2))

  })

  con_db <- DBI::dbConnect(duckdb::duckdb())


})


## delete table ----


describe("delete_table()",{

  it("successfully deletes table",{


    con_db <- DBI::dbConnect(duckdb::duckdb())

    # create input data
    df <- tibble::tibble(id = 1:3, val = c("a","b","c"))


    table_name <- "delete_table"
    DBI::dbWriteTable(con_db,name = table_name,value = df)

    delete_table(con_db,table_name=table_name)


  })



})


## table attributes-------


describe("delete_database()",{

  it("successfully deletes database",{


    con_db <- DBI::dbConnect(duckdb::duckdb())

    # create input data
    df <- tibble::tibble(id = 1:3, val = c("a","b","c"))


    db_name<- "memory"
    table_name <- "delete_db"

    DBI::dbWriteTable(con_db,name = table_name,value = df)

    expect_error(
    delete_database(con_db,database_name = db_name)
    )

  })


})


## list function -----

describe("list_fns()",{

  it("successfully list functions",{

    con_db <- DBI::dbConnect(duckdb::duckdb())

    out <-  list_fns(con_db) |> dplyr::collect()

    expect_true(all(nrow(out)>1))

  })

})


## delete schema -----


describe("delete_schema()",{

  it("successfully deletes schema",{


    con_db <- DBI::dbConnect(duckdb::duckdb())

    # create input data
    df <- tibble::tibble(id = 1:3, val = c("a","b","c"))

    db_name<- "memory"
    table_name <- "delete_schema"
    schema_name <- "test_schema"

    md::create_schema(con_db,database_name = "memory",schema_name = schema_name)

    DBI::dbExecute(con_db, "USE test_schema")
    DBI::dbWriteTable(con_db,name = table_name,value = df)

    delete_schema(con_db,database_name = db_name,schema_name = schema_name,cascade = TRUE)

    new_schema_tbl <- list_current_schemas(con_db) |> dplyr::collect()

    testthat::expect_true(!all(new_schema_tbl[["schema_name"]] %in% schema_name))

    DBI::dbDisconnect(con_db)

  })


})

## read_csv---------

describe("read_csv",{

  it("successfully reads a csv and copies table to database",{

    mtcars |>
      write.csv("mtcars.csv")

    con_db <- DBI::dbConnect(duckdb::duckdb())

    md::read_csv(.con = con_db,to_database_name = "memory",to_schema_name = "main",to_table_name = "mtcars_csv",file_path = "mtcars.csv",write_type = "overwrite")

    fs::file_delete("mtcars.csv")
    list_of_tables <- list_all_tables(con_db) |> dplyr::collect()

    testthat::expect_true(all(list_of_tables[["table_name"]] %in% "mtcars_csv"))

    DBI::dbDisconnect(con_db)



  })
})


## read_excel-----



  describe("read_excel",{

    it("successfully reads a excel and copies table to database",{

      mtcars |>
        openxlsx::write.xlsx("mtcars.xlsx")

      con_db <- DBI::dbConnect(duckdb::duckdb())

      md::read_excel(.con = con_db,to_database_name = "memory",to_schema_name = "main",to_table_name = "mtcars_excel",file_path = "mtcars.xlsx",write_type = "overwrite")

      fs::file_delete("mtcars.xlsx")
      list_of_tables <- list_all_tables(con_db) |> dplyr::collect()

      testthat::expect_true(all(list_of_tables[["table_name"]] %in% "mtcars_excel"))

      DBI::dbDisconnect(con_db)



    })
})
