
#' @title Read an Excel file into a DuckDB/MotherDuck table
#' @name read_excel
#'
#' @description
#' Loads the DuckDB **excel** extension and creates a table from an Excel file
#' using the `read_xlsx()` table function. The destination is fully qualified
#' as `<database>.<schema>.<table>`. Only the options you supply are forwarded
#' to `read_xlsx()` (e.g., `sheet`, `header`, `all_varchar`, `ignore_errors`,
#' `range`, `stop_at_empty`, `empty_as_varchar`).
#' See 'duckdb extension [read_excel](https://duckdb.org/docs/stable/core_extensions/excel) for more information
#'
#' @inheritParams validate_con
#' @param to_database_name Target database name (new or existing)
#' @param to_schema_name Target schema name  (new or existing)
#' @param to_table_name Target table name to create  (new or existing)
#' @param file_path Path to the Excel file (`.xlsx`)
#' @param header Logical; if `TRUE`, first row is header
#' @param sheet Character; sheet name to read (defaults to first sheet)
#' @param all_varchar Logical; coerce all columns to `VARCHAR`
#' @param ignore_errors Logical; continue on cell/row errors
#' @param range Character; Excel range like `"A1"` or `"A1:C100"`
#' @param stop_at_empty Logical; stop at first completely empty row
#' @param empty_as_varchar Logical; treat empty columns as `VARCHAR`
#' @param write_type Logical, will drop previous table and replace with new table
#' @export
#'
#' @return Invisibly returns `NULL`.
#' Side effect: creates `<database>.<schema>.<table>` with the Excel data.
#'
#' @family db-read
read_excel <- function(
        .con
        ,to_database_name
        ,to_schema_name
        ,to_table_name
        ,file_path
        ,header
        ,sheet
        ,all_varchar
        ,ignore_errors
        ,range
        ,stop_at_empty
        ,empty_as_varchar
        ,write_type
        ){

    # stop_at_empty <- TRUE
    # range <- "a1"
    # all_varchar <- TRUE
    # header <-TRUE
    # sheet <- "sheet1"
    # to_database_name="vignette"
    # to_schema_name="raw"
    # to_table_name="starwars"
    # file_path = "starwars.xlsx"


    write_type <- rlang::arg_match(write_type,c("overwrite","append"))
    # file path check and quotation

    assertthat::assert_that(is.character(file_path),file.exists(file_path))

    file_path <- DBI::dbQuoteIdentifier(conn = .con,x = file_path)

    # cell range validation

    if(!missing(range)){

    assertthat::assert_that(is.character(range))

        range_vec <- ",range={range}"

    }else{

        range_vec <- ''
    }

    # stop_at_empty args validation

    if(!missing(stop_at_empty)){

        assertthat::assert_that(is.logical(stop_at_empty))

        stop_at_empty <- if (stop_at_empty) "true" else "false"

        stop_at_empty_vec <- ",stop_at_empty={`stop_at_empty`}"

    }else{

        stop_at_empty_vec <- ''
    }

    # all_varchar validation

    if(!missing(all_varchar)){

        assertthat::assert_that(is.logical(all_varchar))

        all_varchar <- if (all_varchar) "true" else "false"


        all_varchar_vec <- ",all_varchar={all_varchar}"

    }else{

        all_varchar_vec <- ''
    }


    # header args

    if(!missing(header)){

        assertthat::assert_that(is.logical(header))

        header <- if (header) "true" else "false"

        header_vec <- ",header={header}"

    }else{

        header_vec <- ''
    }

    # empty as varcar

    if(!missing(empty_as_varchar)){

        assertthat::assert_that(is.logical(empty_as_varchar))

        empty_as_varchar <- if (header) "true" else "false"

        empty_as_varchar_vec <- ",empty_as_varchar={empty_as_varchar}"

    }else{

        empty_as_varchar_vec <- ''
    }

    # ignore errors

    if(!missing(ignore_errors)){

        assertthat::assert_that(is.logical(ignore_errors))

        ignore_errors <- if (ignore_errors) "true" else "false"

        ignore_errors_vec <- ",ignore_errors={ignore_errors}"

    }else{

        ignore_errors_vec <- ''
    }

    # sheet args

    if(!missing(sheet)){

        assertthat::assert_that(is.character(sheet))

        sheet <- DBI::dbQuoteIdentifier(conn=.con,x=sheet)
        sheet_vec <- ",sheet={`sheet`}"

    }else{

        sheet_vec <- ''
    }

    #validate connection
    validate_con(.con)

    # need to convert logic values to lowercase chracter

    load_extensions(.con,"excel")

    valid_md <- validate_md_connection_status(.con,return_type = "arg")

    if(valid_md){
    #need this to be create if not exists and then use
    DBI::dbExecute(conn = .con,glue::glue_sql("CREATE DATABASE IF NOT EXISTS {`to_database_name`}; USE {`to_database_name`};",.con=.con))
    }

    #need this to be create if not exists and then use
    DBI::dbExecute(conn = .con,glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`to_schema_name`}; USE {`to_schema_name`}; ",.con=.con))

    if(write_type=="overwrite"){

        DBI::dbExecute(
            conn = .con,
            glue::glue_sql("DROP TABLE IF EXISTS {`to_table_name`};", .con = .con)
        )

        DBI::dbExecute(
            conn = .con
            ,glue::glue(
                "CREATE TABLE IF NOT EXISTS {`to_table_name`} AS SELECT * FROM read_xlsx({file_path}",range_vec,stop_at_empty_vec,all_varchar_vec,header_vec,sheet_vec,ignore_errors_vec,empty_as_varchar_vec,");"
            )
            ,.con = .con
        )}

    if(write_type=="append"){

        DBI::dbExecute(
            conn = .con
            ,glue::glue(
                "CREATE TABLE IF NOT EXISTS {`to_table_name`}; INSERT INTO {`to_table_name`} SELECT * FROM read_xlsx({file_path}",range_vec,stop_at_empty_vec,all_varchar_vec,header_vec,sheet_vec,ignore_errors_vec,empty_as_varchar_vec,");"
            )
            ,.con = .con
        )}

    cli_create_obj(.con,database_name = to_database_name,schema_name = to_schema_name,table_name = to_table_name,write_type = write_type)
}




#' @title Read a CSV file into a DuckDB/MotherDuck table
#' @name read_csv
#'
#' @description
#' Loads the DuckDB **excel** extension and creates a table from a CSV file
#' using the `read_csv_auto()` table function. The destination is fully qualified
#' as `<database>.<schema>.<table>`. Only the options you supply are forwarded
#' to `read_csv_auto()` (e.g., `header`, `all_varchar`, `sample_size`,
#' `names`, `types`, `skip`, `union_by_name`, `normalize_names`,
#' `allow_quoted_nulls`, `ignore_errors`). If `names` or `types` are not supplied,
#' they are ignored. See the DuckDB [read_csv_auto() documentation](https://duckdb.org/docs/stable/data/csv/overview) for more information.
#'
#' @inheritParams validate_con
#' @inheritParams read_excel
#' @param sample_size Numeric; number of rows used for type inference
#' @param names Character vector; optional column names to assign instead of reading from the file
#' @param types Named or unnamed character vector; column types (named preferred, unnamed paired to `names`)
#' @param skip Integer; number of rows to skip at the beginning of the file
#' @param union_by_name Logical; union multiple CSVs by column name
#' @param normalize_names Logical; normalize column names (lowercase, replace spaces)
#' @param allow_quoted_nulls Logical; treat `"NULL"` in quotes as NULL
#' @param ignore_errors Logical; continue on row parse errors
#' @param write_type Character; either `"overwrite"` or `"append"`, controls table creation behavior
#' @param ... Additional arguments passed to `read_csv_auto()` in format listed in duckdb documentation (optional)
#'
#' @return Invisibly returns `NULL`. Side effect: creates `<database>.<schema>.<table>` with the CSV data
#'
#' @family db-read
#' @export
read_csv <- function(
        .con
        ,to_database_name
        ,to_schema_name
        ,to_table_name
        ,file_path
        ,header
        ,all_varchar
        ,sample_size
        ,names
        ,types
        ,skip
        ,union_by_name
        ,normalize_names
        ,allow_quoted_nulls
        ,ignore_errors
        ,write_type
        ,...
        ){

    # stop_at_empty <- TRUE
    # all_varchar <- TRUE
    # header <-TRUE
    # to_database_name="vignette"
    # to_schema_name="raw"
    # to_table_name="starwars"
    # file_path = "starwars.xlsx"


    write_type <- rlang::arg_match(write_type,c("overwrite","append"))
    # file path check and quotation

    assertthat::assert_that(is.character(file_path),file.exists(file_path))

    file_path <- DBI::dbQuoteIdentifier(conn = .con,x = file_path)




    # --- header ---
    if (!missing(header)) {
        assertthat::assert_that(is.logical(header))
        header <- if (header) "true" else "false"
        header_vec <- glue::glue(", header={header}")
    } else {
        header_vec <- ""
    }

    # --- all_varchar ---
    if (!missing(all_varchar)) {
        assertthat::assert_that(is.logical(all_varchar))
        all_varchar <- if (all_varchar) "true" else "false"
        all_varchar_vec <- glue::glue(", all_varchar={all_varchar}")
    } else {
        all_varchar_vec <- ""
    }



    # --- sample_size ---
    if (!missing(sample_size)) {
        assertthat::assert_that(is.numeric(sample_size) && length(sample_size) == 1)
        sample_size_vec <- glue::glue(", sample_size={sample_size}")
    } else {
        sample_size_vec <- ""
    }

    # --- names: return empty string if missing or NULL ---
    if (missing(names) || is.null(names)) {
        names_vec <- ""
    } else {
        assertthat::assert_that(is.character(names), length(names) > 0)
        # format: ['A', 'B', 'C']
        names_str <- paste0("['", paste(names, collapse = "', '"), "']")
        names_vec <- glue::glue(", names = {names_str}")
    }

    # --- types: accept named vector or unnamed vector (paired to names) ---
    if (missing(types) || is.null(types)) {
        types_vec <- ""
    } else {
        assertthat::assert_that(is.character(types), length(types) > 0)

        # If types are unnamed but names provided, pair positionally
        if (is.null(names(types))) {
            if (!missing(names) && !is.null(names)) {
                if (length(types) != length(names)) {
                    stop("When `types` is unnamed it must have the same length as `names`.")
                }
                types_named <- stats::setNames(as.character(types), names)
            } else {
                stop("`types` must be a named character vector, or an unnamed vector paired with `names`.")
            }
        } else {
            types_named <- as.character(types)
        }

        # Build the columns = {'col':'TYPE', 'col2':'TYPE2'} fragment
        cols_pairs <- paste0("'", names(types_named), "': '", types_named, "'")
        cols_body <- paste(cols_pairs, collapse = ", ")
        types_vec <- glue::glue(", columns = {{{cols_body}}}")
    }



    # --- skip ---
    if (!missing(skip)) {
        assertthat::assert_that(is.numeric(skip) && length(skip) == 1)
        skip_vec <- glue::glue(", skip={skip}")
    } else {
        skip_vec <- ""
    }



    # --- union_by_name ---
    if (!missing(union_by_name)) {
        assertthat::assert_that(is.logical(union_by_name))
        union_by_name <- if (union_by_name) "true" else "false"
        union_by_name_vec <- glue::glue(", union_by_name={union_by_name}")
    } else {
        union_by_name_vec <- ""
    }



    # --- normalize_names ---
    if (!missing(normalize_names)) {
        assertthat::assert_that(is.logical(normalize_names))
        normalize_names <- if (normalize_names) "true" else "false"
        normalize_names_vec <- glue::glue(", normalize_names={normalize_names}")
    } else {
        normalize_names_vec <- ""
    }


    # --- allow_quoted_nulls ---
    if (!missing(allow_quoted_nulls)) {
        assertthat::assert_that(is.logical(allow_quoted_nulls))
        allow_quoted_nulls <- if (allow_quoted_nulls) "true" else "false"
        allow_quoted_nulls_vec <- glue::glue(", allow_quoted_nulls={allow_quoted_nulls}")
    } else {
        allow_quoted_nulls_vec <- ""
    }

    # --- ignore_errors ---
    if (!missing(ignore_errors)) {
        assertthat::assert_that(is.logical(ignore_errors))
        ignore_errors <- if (ignore_errors) "true" else "false"
        ignore_errors_vec <- glue::glue(", ignore_errors={ignore_errors}")
    } else {
        ignore_errors_vec <- ""
    }



    #validate connection
    validate_con(.con)

    valid_md <- validate_md_connection_status(.con,return_type = "arg")

    # need to convert logic values to lowercase chracter


    if(valid_md){
    #need this to be create if not exists and then use
    DBI::dbExecute(conn = .con,glue::glue_sql("CREATE DATABASE IF NOT EXISTS {`to_database_name`}; USE {`to_database_name`};",.con=.con))
    }

    #need this to be create if not exists and then use
    DBI::dbExecute(conn = .con,glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`to_schema_name`}; USE {`to_schema_name`}; ",.con=.con))

    if(write_type=="overwrite"){

        DBI::dbExecute(
            conn = .con,
            glue::glue_sql("DROP TABLE IF EXISTS {`to_table_name`};", .con = .con)
        )

        if(!rlang::is_missing(...)){

            DBI::dbExecute(
                conn = .con
                ,glue::glue(
                    "CREATE TABLE IF NOT EXISTS {`to_table_name`} AS SELECT * FROM read_csv_auto({file_path}",header_vec,all_varchar_vec,sample_size_vec,names_vec,types_vec,skip_vec,union_by_name_vec,normalize_names_vec,allow_quoted_nulls_vec,ignore_errors_vec,...,");"
                )
                ,.con = .con
            )

        }else{

        DBI::dbExecute(
            conn = .con
            ,glue::glue(
                "CREATE TABLE IF NOT EXISTS {`to_table_name`} AS SELECT * FROM read_csv_auto({file_path}",header_vec,all_varchar_vec,sample_size_vec,names_vec,types_vec,skip_vec,union_by_name_vec,normalize_names_vec,allow_quoted_nulls_vec,ignore_errors_vec,");"
            )
            ,.con = .con
        )
        }
    }

    if(write_type=="append"){

        if(!missing(...)){

            DBI::dbExecute(
                conn = .con
                ,glue::glue(
                    "CREATE TABLE IF NOT EXISTS {`to_table_name`} AS SELECT * FROM read_csv_auto({file_path}",header_vec,all_varchar_vec,sample_size,names_vec,types_vec,skip_vec,union_by_name_vec,normalize_names_vec,allow_quoted_nulls_vec,ignore_errors_vec,...,");"
                )
                ,.con = .con
            )

        }else{

            DBI::dbExecute(
                conn = .con
                ,glue::glue(
                    "CREATE TABLE IF NOT EXISTS {`to_table_name`} AS SELECT * FROM read_csv_auto({file_path}",header_vec,all_varchar_vec,sample_size_vec,names_vec,types_vec,skip_vec,union_by_name_vec,normalize_names_vec,allow_quoted_nulls_vec,ignore_errors_vec,");"
                )
                ,.con = .con
            )}


    }
    cli_create_obj(.con,database_name = to_database_name,schema_name = to_schema_name,table_name = to_table_name,write_type = write_type)

    }

