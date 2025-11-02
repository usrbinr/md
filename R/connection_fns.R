
#' @title Validate connection is DuckDB
#' @name validate_con
#' @description
#' Validates that your connection object is a DuckDB connection
#'
#' @param .con  A valid `DBI` connection (DuckDB / MotherDuck).
#
#' @returns logical value or error message
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#' validate_duckdb_con(con)
#' }
validate_con <- function(.con){

    dbIsValid_poss <- purrr::possibly(DBI::dbIsValid)


    valid_test <- is.null(dbIsValid_poss(.con))

    if(valid_test){

        cli::cli_abort("Connection string is not valid, please try again")

    }else{

        return(TRUE)

    }
}



#' @title Validate Mother Duck Connection Status
#' @name validate_md_connection_status
#' @description
#' Validates if you are successfully connected to motherduck database and will return either a logical value or print a message
#'
#'
#' @inheritParams validate_con
#' @param return_type return message or logical value of connection status
#' @seealso [validate_con()] and [connect_to_motherduck()]
#'
#' @returns logical value or warning message
#' @export
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#' validate_md_connection_status(con)
#' }
validate_md_connection_status <- function(.con,return_type="msg"){

    # return_type <- "msg"
    return_type <- rlang::arg_match(
        return_type
        ,values  = c("msg","arg")
    )

    validate_con(.con)

    dbExectue_safe <- purrr::safely(DBI::dbExecute)

    out <- dbExectue_safe(.con, "PRAGMA MD_CONNECT")

    status_lst <- list()

    if(any(stringr::str_detect(out$error$message,"Error: already connected")==TRUE)){

        status_lst$msg <-  \(x) cli::cli_alert_success("You are connected to MotherDuck")
        status_lst$arg <- TRUE

    }else{


        status_lst$msg <-\(x) cli::cli_alert_warning("You are not connected to MotherDuck")
        status_lst$arg <- FALSE

    }

    if(return_type=="msg"){

        cli::cli_h2(text="Connection Status Report:")
        return(status_lst$msg())

    }else{

        return(status_lst$arg)

    }

}

#' @title Create connection to motherduck
#' @name  connect_to_motherduck
#'
#' @description
#' Establishes a connection to a MotherDuck account using DuckDB and the MotherDuck extension.
#' The function handles token validation, database file creation, extension loading, and executes
#' `PRAGMA MD_CONNECT` to authenticate the connection.
#'
#' @details
#' This function provides a convenient interface for connecting to MotherDuck. It allows you to:
#' * Use a token stored in an environment variable or supply the token directly.
#' * Optionally specify a persistent DuckDB database file or directory via `db_path`.
#' * Optionally Provide custom DuckDB configuration options via `config`.
#' * Automatically load the MotherDuck extension if not already loaded.
#'
#' If `db_path` is not supplied, a temporary DuckDB database file will be created in the session's
#' temporary directory. Use `config` to pass any DuckDB-specific options (e.g., memory limits or
#' extensions).
#'
#' @param motherduck_token Character. Either the name of an environment variable containing your
#'   MotherDuck access token (default `"MOTHERDUCK_TOKEN"`) or the token itself.
#' @param db_path Character, optional. Path to a DuckDB database file or directory to use. If
#'   `NULL`, a temporary file is used.
#' @param config List, optional. A list of DuckDB configuration options to be passed to
#'   `duckdb::duckdb()`.
#'
#' @return A `DBIConnection` object connected to your MotherDuck account.
#'
#' @examples
#' \dontrun{
#' # Connect using a token stored in your .Renviron
#' con <- connect_to_motherduck()
#'
#' # Connect with a direct token
#' con <- connect_to_motherduck(motherduck_token = "MY_DIRECT_TOKEN")
#'
#' # Connect and specify a persistent database file
#' con <- connect_to_motherduck(db_path = "~/my_duckdb/md.duckdb")
#' }
#'
#' @export
connect_to_motherduck <- function(motherduck_token="MOTHERDUCK_TOKEN",db_path=NULL,config){

     # test
     # motherduck_token="MOTHERDUCK_TOKEN"


    #confirm config is a list

    if(!missing(config)){
        assertthat::assert_that(is.list(config))
    }


    ## allows for user to either directly input their token if not available

    if(nchar(Sys.getenv(motherduck_token))>1){

    motherduck_token_code <- Sys.getenv(motherduck_token)

    }

    cli_msg <- function() {
        cli::cli_par()
        cli::cli_h1("Motherduck token status")
        cli::cli_ul()
        cli::cli_li("Enter the variable name that matches the MotherDuck token variable assigned in your {.file ~/.Renviron}. or alternative environment file")
        cli::cli_li("In {.url https://www.motherduck.com}, goto 'Settings' > 'Integrations' and click 'Access Tokens' to create a new token")
        cli::cli_li("Use {.fn usethis::edit_r_environ} to open and edit your {.file ~/.Renviron} file.")
        cli::cli_li("Enter {.code MOTHERDUCK_TOKEN = ...} in your {.file ~/.Renviron} file and save")
        cli::cli_li("Pass {.envvar MOTHERDUCK_TOKEN} to {.fn connect_to_motherduck}")
        cli::cli_end()
        cli::cli_par()
        cli::cli_end()
    }

    assertthat::assert_that(
        motherduck_token_code!=""
        ,msg=cli_msg()
    )


    # Use provided dbdir or fallback to temp file
    db_path <- if (!is.null(db_path)) {
        db_path <- db_path
    } else {
        db_path <-   tempfile(fileext = ".duckdb")
    }


    if(!missing(config)){

    .con <- DBI::dbConnect(duckdb::duckdb(dbdir = db_path))


    }else{
        .con <-DBI::dbConnect(duckdb::duckdb(dbdir = db_path))
    }



    if(!validate_extension_load_status(.con,"motherduck",return_type="arg")){

        load_extensions(.con,"motherduck")

    }

    if(!validate_md_connection_status(.con,return_type = "arg")){

    DBI::dbExecute(.con, paste0("SET motherduck_token=", DBI::dbQuoteString(.con, motherduck_token_code), ";"))

    }

    sql_statements <- vapply(
        names(config),
        function(x) sprintf("SET %s='%s';", x, config[[x]]),
        character(1)
    )

    for (stmt in sql_statements) {
        DBI::dbExecute(.con, stmt)
    }

    # connect to motherduck

    dbExectue_safe <- purrr::safely(DBI::dbExecute)

    dbExectue_safe(.con, "PRAGMA MD_CONNECT")

    validate_md_connection_status(.con,return_type = "msg")

    return(.con)

}



#' @title validate and Pprint your database location
#' @name validate_and_print_database_loction
#' @description
#' Internal function to help validate your local database location
#'
#' @inheritParams validate_con
#'
#' @returns print message
#'
validate_and_print_database_loction <- function(.con){

    validate_con(.con)

    file_location <- .con@driver@dbdir

    assertthat::assert_that(
        file.exists(file_location)
        ,msg = cli::format_error("Failed to find file or database at {.file {file_location}}")
    )
    cli::cli_alert_success("created or located a database at {.file {file_location}}")
}
