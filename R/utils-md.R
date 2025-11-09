
#' @title List MotherDuck/DuckDB Extensions
#' @name list_extensions
#'
#' @description
#' Retrieves all available DuckDB or MotherDuck extensions along with their descriptions,
#' installation and load status.
#'
#' @details
#' The `list_extensions()` function queries the database for all extensions that are
#' available in the current DuckDB or MotherDuck connection. The returned tibble includes
#' information such as:
#' - `extension_name`: Name of the extension.
#' - `description`: Short description of the extension.
#' - `installed`: Logical indicating if the extension is installed.
#' - `loaded`: Logical indicating if the extension is currently loaded.
#'
#' This is useful for determining which extensions can be installed or loaded using
#' `install_extensions()` or `load_extensions()`.
#'
#' @inheritParams validate_con
#'
#' @returns A tibble with one row per extension and columns describing its metadata
#' and current status.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # List all available extensions
#' list_extensions(con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso
#' [load_extensions()], [install_extensions()],[validate_extension_install_status()],[validate_extension_load_status()],[validate_extension_install_status()]
#'
#' @export
list_extensions <- function(.con){

  validate_con(.con)

  out <- DBI::dbGetQuery(
    .con,
    "
    SELECT *
    FROM duckdb_extensions()
    "
  ) |> tibble::as_tibble()

  return(out)

}




#' @title Validate Loaded MotherDuck/DuckDB Extensions
#' @name validate_extension_load_status
#'
#' @description
#' Checks whether specified DuckDB or MotherDuck extensions are loaded in the current session and provides a detailed status report.
#'
#' @details
#' The `validate_extension_load_status()` function validates the current connection, then
#' checks which of the requested extensions are loaded. It produces a detailed CLI report
#' showing which extensions are loaded, failed to load, or missing.
#'
#' Depending on the `return_type` argument, the function can either print messages, return
#' a list of extension statuses, or return a logical value indicating whether all requested
#' extensions are successfully loaded.
#'
#' @inheritParams validate_con
#' @param extension_names A character vector of extensions to validate.
#' @param return_type One of `"msg"`, `"ext"`, or `"arg"`. Determines the type of return value:
#'   - `"msg"` prints CLI messages.
#'   - `"ext"` returns a list with `success_ext`, `fail_ext`, and `missing_ext`.
#'   - `"arg"` returns TRUE if all requested extensions are loaded, FALSE otherwise.
#'
#' @returns
#' Depending on `return_type`:
#' - `"msg"`: prints CLI messages (invisible `NULL`).
#' - `"ext"`: list with `success_ext`, `fail_ext`, and `missing_ext`.
#' - `"arg"`: logical indicating if all requested extensions are loaded.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Print CLI report
#' validate_extension_load_status(con, extension_names = c("excel", "arrow"), return_type = "msg")
#'
#' # Return a list of loaded, failed, and missing extensions
#' validate_extension_load_status(con, extension_names = c("excel", "arrow"), return_type = "ext")
#'
#' # Return logical indicating if all requested extensions are loaded
#' validate_extension_load_status(con, extension_names = c("excel", "arrow"), return_type = "arg")
#' }
#'
#' @seealso
#' [load_extensions()], [list_extensions()],[install_extensions()],[validate_extension_install_status()]
#'
#' @export
validate_extension_load_status <- function(.con,extension_names,return_type="msg"){


  # extension_names <- "motherduck"

  return_type <- rlang::arg_match(
    return_type
    ,values = c("msg","ext","arg")
    ,multiple = FALSE
    ,error_arg = "Please only select 'msg', 'ext' or 'arg'"
  )


  # validate duckdb connection
  validate_con(.con)

  # validate valid extensions

  valid_ext_vec <- list_extensions(.con) |> dplyr::pull(extension_name)

  # pull status of the named exstensions
  ext_tbl <- list_extensions(.con) |>
    dplyr::filter(
      extension_name %in% extension_names
    ) |>
    dplyr::select(extension_name,loaded)

  # create a list to capture results
  ext_lst <- list()

  # assign extension status
  ext_lst$success_ext <- ext_tbl |>
    dplyr::filter(
      loaded==TRUE
    ) |>
    dplyr::pull(extension_name)

  ext_lst$fail_ext <- ext_tbl |>
    dplyr::filter(
      loaded==FALSE
    ) |>
    dplyr::pull(extension_name)

  ext_lst$missing_ext <- extension_names[!extension_names%in% valid_ext_vec]

  # create a list of extensions messages
  msg_lst <- list()

  if(length(ext_lst$missing_ext)>0){
    msg_lst$missing_ext <- "{.pkg {ext_lst$missing_ext}} can't be found"
  }

  if(length(ext_lst$success_ext)>0){

    msg_lst$sucess_ext <- "{.pkg {ext_lst$success_ext}} loaded"
  }

  if(length(ext_lst$fail_ext)>0){
    msg_lst$fail_ext <- "{.pkg {ext_lst$fail_ext}} did not load"
  }


  # create message
  cli_ext_status_msg <- function() {
    cli::cli_par()
    cli::cli_h1("Extension load status")
    purrr::map(
      msg_lst
      ,.f = \(x) cli::cli_text(x)
    )
    cli::cli_end()
    cli::cli_par()
    cli::cli_text("Use {.fn list_extensions} to list extensions and their descriptions")
    cli::cli_text("Use {.fn install_extensions} to install new {cli::col_red('duckdb')} extensions")
    cli::cli_text("See {.url https://duckdb.org/docs/stable/extensions/overview.html} for more information")
    cli::cli_end()
  }


  if(!length(ext_lst$fail_ext)>0){

    status <- TRUE

  }else{
    status <- FALSE
  }

  if(return_type=="msg"){

    cli_ext_status_msg()

  }

  if(return_type=="ext"){
    return(ext_lst)
  }

  if(return_type=="arg"){
    return(status)
  }

}





#' @title Validate Installed MotherDuck/DuckDB Extensions
#' @name validate_extension_install_status
#'
#' @description
#' Checks whether specified DuckDB or MotherDuck extensions are installed and provides a detailed status report.
#'
#' @details
#' The `validate_extension_install_status()` function validates the current connection and
#' checks which of the requested extensions are installed. It produces a detailed CLI report
#' showing which extensions are installed, not installed, or missing.
#'
#' The function can return different outputs based on the `return_type` argument:
#' - `"msg"`: prints a CLI report with extension statuses.
#' - `"ext"`: returns a list containing `success_ext` (installed) and `fail_ext` (not installed).
#' - `"arg"`: returns a logical value indicating whether all requested extensions are installed.
#'
#' @inheritParams validate_con
#' @param extension_names A character vector of extensions to validate.
#' @param return_type One of `"msg"`, `"ext"`, or `"arg"`. Determines the type of return value.
#'   - `"msg"` prints CLI messages.
#'   - `"ext"` returns a list of installed and failed extensions.
#'   - `"arg"` returns TRUE if all requested extensions are installed, FALSE otherwise.
#'
#' @returns
#' Depending on `return_type`:
#' - `"msg"`: prints CLI messages (invisible `NULL`).
#' - `"ext"`: list with `success_ext` and `fail_ext`.
#' - `"arg"`: logical indicating if all requested extensions are installed.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Print CLI report
#' validate_extension_install_status(con, extension_names = c("arrow", "excel"), return_type = "msg")
#'
#' # Return a list of installed and failed extensions
#' validate_extension_install_status(con, extension_names = c("arrow", "excel"), return_type = "ext")
#'
#' # Return logical indicating if all requested extensions are installed
#' validate_extension_install_status(con, extension_names = c("arrow", "excel"), return_type = "arg")
#' }
#'
#' @seealso
#' [load_extensions()], [list_extensions()],[install_extensions()],[validate_extension_load_status()],[validate_extension_install_status()]
#'
#' @export
validate_extension_install_status <- function(.con,extension_names,return_type="msg"){

  ## need to first validate those that are returned from the table
  ## Then filter against the vector for those that aren't in table
  # extension_names <- c("arrow","excel","Adsfd","motherduck")


  return_type <- rlang::arg_match(
    return_type
    ,values = c("msg","ext","arg")
    ,multiple = FALSE
    ,error_arg = "return_type"
  )

  validate_con(.con)

  valid_ext_vec <- list_extensions(.con) |> dplyr::pull(extension_name)

  ext_tbl <- list_extensions(.con) |>
    dplyr::filter(
      extension_name %in% c(extension_names)
    ) |>
    dplyr::select(extension_name,installed)




  ext_lst <- list()
  # load exntesion status
  ext_lst$success_ext <- ext_tbl |>
    dplyr::filter(
      installed==TRUE
    ) |>
    dplyr::pull(extension_name)

  ext_lst$fail_ext <- ext_tbl |>
    dplyr::filter(
      installed==FALSE
    ) |>
    dplyr::pull(extension_name)

  msg_lst <- list()

  if(length(ext_lst$missing_ext)>0){
    msg_lst$missing_ext <- "{.pkg {ext_lst$fail_ext}} can't be found"
  }

  if(length(ext_lst$success_ext)>0){

    msg_lst$sucess_ext <- "{.pkg {ext_lst$success_ext}} is installed"
  }

  if(length(ext_lst$fail_ext)>0){
    msg_lst$fail_ext <- "{.pkg {ext_lst$fail_ext}} is not installed"
  }


  # create message
  cli_ext_status_msg <- function() {
    cli::cli_par()
    cli::cli_h1("Extension install status")

    purrr::map(
      msg_lst
      ,.f = \(x) cli::cli_text(x)
    )

    cli::cli_end()
    cli::cli_par()
    cli::cli_text("Use {.fn list_extensions} to list extensions and their descriptions")
    cli::cli_text("Use {.fn install_extensions} to install new {cli::col_red('duckdb')} extensions")
    cli::cli_text("See {.url https://duckdb.org/docs/stable/extensions/overview.html} for more information")
    cli::cli_end()
  }

  # check

  if(!length(ext_lst$fail_ext)>0){

    status <- TRUE

  }else{
    status <- FALSE
  }

  if(return_type=="msg"){

    cli_ext_status_msg()

  }

  if(return_type=="ext"){
    return(ext_lst)
  }

  if(return_type=="arg"){
    return(status)
  }

}



#' @title Install DuckDB/MotherDuck Extensions
#' @name install_extensions
#'
#' @description
#' Installs valid DuckDB or MotherDuck extensions for the current connection.
#'
#' @details
#' The `install_extensions()` function validates the provided DuckDB/MotherDuck connection,
#' then checks which of the requested extensions are valid. Valid extensions that are not
#' already installed are installed using the `INSTALL` SQL command. Invalid extensions are
#' reported to the user via CLI messages. This function provides a summary report
#' describing which extensions were successfully installed and which were invalid.
#'
#' Unlike `load_extensions()`, this function focuses purely on installation and does not
#' automatically load extensions after installing.
#'
#' @inheritParams validate_con
#' @param extension_names A character vector of DuckDB/MotherDuck extension names to install.
#'
#' @returns Invisibly returns `NULL`. A detailed CLI report of installation success/failure
#' is printed.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Install the 'motherduck' extension
#' install_extensions(con, "motherduck")
#'
#' # Install multiple extensions
#' install_extensions(con, c("fts", "httpfs"))
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso
#' [load_extensions()], [list_extensions()],[validate_extension_install_status()],[validate_extension_load_status()]
#'
#' @export
install_extensions <- function(.con,extension_names){

  # extension_names <- c("fts")
  # silent_msg <- TRUE
  # .con <- con

  # assertthat::assert_that(is.logical(silent_msg),msg = "silent_msg must be TRUE or FALSE")

  validate_con(.con)

  valid_ext_vec <- list_extensions(.con) |>
    dplyr::pull(extension_name)

 ext_lst <- list()

 ext_lst$invalid_ext <-  extension_names[!extension_names%in%valid_ext_vec ]

 ext_lst$valid_ext <- extension_names[extension_names%in%valid_ext_vec ]

  # install packages

 # validate_extension_install_status(.con,ext_lst$valid_ext,return_type = "arg")

 if(!validate_extension_install_status(.con,ext_lst$valid_ext,return_type = "arg")){

   purrr::map(
     ext_lst$valid_ext
     ,\(x)  DBI::dbExecute(.con, glue::glue("INSTALL {x};"))
   )

 }

  msg_lst <- list()

  if(length(ext_lst$valid_ext)>0){
    n_ext <- length(ext_lst$valid_ext)
    msg_lst$valid_msg <- "Installed {cli::col_yellow({cli::no(n_ext)})} extension{?s}: {.pkg {ext_lst$valid_ext}}"

  }

  if(length(ext_lst$invalid_ext)>0){
    n_ext <- length(ext_lst$invalid_ext)
    msg_lst$invalid_msg <- "Failed to install {cli::col_yellow({cli::no(n_ext)})} extension{?s}: {.pkg {ext_lst$invalid_ext}} are not valid"

  }


  cli_ext_status_msg <- function() {
    cli::cli_par()
    cli::cli_h1("Extension Install Report")

    purrr::map(
      msg_lst
      ,.f = \(x) cli::cli_text(x)
    )

    cli::cli_end()
    cli::cli_par()
    cli::cli_text("Use {.fn list_extensions} to list extensions, status and their descriptions")
    cli::cli_text("Use {.fn install_extensions} to install new {cli::col_red('duckdb')} extensions")
    cli::cli_text("See {.url https://duckdb.org/docs/stable/extensions/overview.html} for more information")
    cli::cli_end()
  }
# if(!silent_msg){
  cli_ext_status_msg()
# }

}


#' @title Load and Install DuckDB/MotherDuck Extensions
#' @name load_extensions
#'
#' @description
#' Installs (if necessary) and loads valid DuckDB or MotherDuck extensions for the active connection.
#'
#' @details
#' The `load_extensions()` function first validates the provided DuckDB/MotherDuck connection,
#' then checks which of the requested extensions are valid and not already installed.
#' Valid extensions are installed and loaded into the current session. Invalid extensions
#' are reported to the user. The function provides a detailed CLI report summarizing which
#' extensions were successfully installed and loaded, and which were invalid.
#'
#' It is especially useful for ensuring that required extensions, such as `motherduck`,
#' are available in your database session. The CLI messages also provide guidance on
#' listing all available extensions and installing additional DuckDB extensions.
#'
#' @inheritParams validate_con
#' @param extension_names A character vector of DuckDB/MotherDuck extension names to load/install.
#'
#' @returns Invisibly returns `NULL`. The function prints a CLI report of the extension status.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#'
#' # Install and load the 'motherduck' extension
#' load_extensions(con, "motherduck")
#'
#' # Load multiple extensions
#' load_extensions(con, c("motherduck", "httpfs"))
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso
#' [list_extensions()], [install_extensions()]
#'
#' @export
load_extensions <- function(.con,extension_names){

  # extension_names <- c("motherduck")
  # silent_msg <- TRUE
  # .con <- connect_to_motherduck()

  # assertthat::assert_that(is.logical(silent_msg),msg = "silent_msg must be TRUE or FALSE")

  validate_con(.con)

  valid_ext_vec <- list_extensions(.con) |>
    dplyr::pull(extension_name)

  ext_lst <- list()

  ext_lst$invalid_ext <-  extension_names[!extension_names%in%valid_ext_vec ]

  ext_lst$valid_ext <- extension_names[extension_names%in%valid_ext_vec ]

  # install packages

  # validate_extension_install_status(.con,ext_lst$valid_ext,return_type = "arg")

  if(!validate_extension_install_status(.con,ext_lst$valid_ext,return_type = "arg")){


    purrr::map(
      ext_lst$valid_ext
      ,\(x)  DBI::dbExecute(.con, glue::glue("INSTALL {x};"))
    )

  }

  # load packages
  purrr::map(
    ext_lst$valid_ext
    ,\(x)  DBI::dbExecute(.con, glue::glue("LOAD {x};"))
  )

  msg_lst <- list()

  if(length(ext_lst$valid_ext)>0){
    n_ext <- length(ext_lst$valid_ext)
    msg_lst$valid_msg <- "Installed and loaded {cli::col_yellow({cli::no(n_ext)})} extension{?s}: {.pkg {ext_lst$valid_ext}}"

  }

  if(length(ext_lst$invalid_ext)>0){
    n_ext <- length(ext_lst$invalid_ext)
    msg_lst$invalid_msg <- "Failed to install and load {cli::col_yellow({cli::no(n_ext)})} extension{?s}: {.pkg {ext_lst$invalid_ext}} are not valid"

  }


  cli_ext_status_msg <- function() {

    cli::cli_par()
    cli::cli_h1("Extension Load & Install Report")
    purrr::map(
      msg_lst
      ,.f = \(x) cli::cli_text(x)
    )
    cli::cli_end()
    cli::cli_par()
    cli::cli_text("Use {.fn list_extensions} to list extensions, status and their descriptions")
    cli::cli_text("Use {.fn install_extensions} to install new {cli::col_red('duckdb')} extensions")
    cli::cli_text("See {.url https://duckdb.org/docs/stable/extensions/overview.html} for more information")
    cli::cli_end()
  }


    cli_ext_status_msg()


}




#' @title Show Your MotherDuck Token
#' @name show_motherduck_token
#'
#' @description
#' Displays the active MotherDuck authentication token associated with the current connection.
#' Useful for debugging or verifying that your session is authenticated correctly.
#'
#' @details
#' The `show_motherduck_token()` function executes the internal MotherDuck pragma
#' `print_md_token` and returns the token information. This function should only be
#' used in secure environments, as it exposes your authentication token in plain text.
#' It requires a valid MotherDuck connection established with `DBI::dbConnect()`.
#'
#' @inheritParams validate_con
#'
#' @returns A tibble containing the current MotherDuck token.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "md:")
#' show_motherduck_token(con)
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso
#' [connect_to_motherduck]
#'
#' @export
show_motherduck_token <- function(.con){

  validate_con(.con)

  DBI::dbGetQuery(.con, 'PRAGMA print_md_token;')

}


#' @title Print Current MotherDuck Database Context
#' @name pwd
#'
#' @description
#' Displays the current database, schema, and role for the active DuckDB/MotherDuck connection.
#' This mirrors the behavior of `pwd` in Linux by showing your current “working database.”
#'
#' @details
#' The `pwd()` function is a helper for inspecting the current database context of a DuckDB
#' or MotherDuck connection. It queries the database for the current database, schema, and role.
#' The database and schema are returned as a tibble for easy programmatic access, while the
#' role is displayed using a CLI alert. This is especially useful in multi-database environments
#' or when working with different user roles, providing a quick way to verify where SQL queries
#' will be executed.
#'
#' @inheritParams validate_con
#'
#' @returns
#' A tibble with columns:
#' \describe{
#'   \item{current_database}{The active database name.}
#'   \item{current_schema}{The active schema name.}
#' }
#' The current role is printed to the console via `cli`.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb())
#' pwd(con)
#' }
#'
#' @seealso
#' [validate_con()], [list_databases()], [list_schemas()]
#'
#' @export
pwd <- function(.con){

  validate_con(.con)

  database_tbl <-
    DBI::dbGetQuery(.con,"select current_database();") |>
    tibble::as_tibble(.name_repair = janitor::make_clean_names)

  schema_tbl <- DBI::dbGetQuery(.con,"select current_schema();") |>
    tibble::as_tibble(.name_repair = janitor::make_clean_names)

  role_vec <- DBI::dbGetQuery(.con,"select current_role();") |>
    tibble::as_tibble(.name_repair = janitor::make_clean_names) |>
    dplyr::pull(current_role)


  out <- dplyr::bind_cols(database_tbl,schema_tbl)

  cli::cli_alert("Current role: {.envvar {role_vec}}")

  return(out)
}




#' @title Change Active Database and Schema
#' @name cd
#' @description
#' Switches the active database and (optionally) schema for a valid
#' DuckDB or MotherDuck connection. The function validates the target
#' database and schema before executing the `USE` command and provides
#' user feedback via CLI messages.
#'
#' @details
#' The `cd()` function is analogous to a "change directory" command in a
#' file system, but for database contexts. It updates the currently active
#' database (and optionally schema) for the given connection. If the target
#' database or schema does not exist, the function aborts with a descriptive
#' CLI error message.
#'
#' @inheritParams validate_con
#' @param database_name A character string specifying the database to switch to.
#'   Must be one of the available databases returned by [list_databases()].
#' @param schema_name (Optional) A character string specifying the schema
#'   to switch to within the given database. Must be one of the available
#'   schemas returned by [list_schemas()].
#'
#' @returns
#' Invisibly returns a message summarizing the new connection context.
#' Side effects include printing CLI headers showing the current user
#' and database context.
#'
#' @examples
#' \dontrun{
#' # Connect to MotherDuck
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "md:")
#'
#' # List available databases
#' list_databases(con)
#'
#' # Change to a specific database and schema
#' cd(con, database_name = "analytics_db", schema_name = "public")
#'
#' # Disconnect
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso
#' [list_databases()], [list_schemas()],[pwd()]
#'
#' @export
cd <- function(.con,database_name,schema_name){

  validate_con(.con)

  database_valid_vec <- list_databases(.con) |>
    dplyr::pull(database_name)

  if(any(database_name %in% database_valid_vec)){

    DBI::dbExecute(.con,glue::glue("USE {database_name};"))

  }else{

    cli::cli_abort("
                   {.pkg {database}} is not valid,
                   Use {.fn list_databases} to list valid databases.
                   Valid databases are: {.val {database_valid_vec}}
                   ")
  }

  if(!missing(schema_name)){

  schema_valid_vec <- list_schemas(.con) |>
    dplyr::pull(schema_name)


  if(any(schema_name %in% schema_valid_vec)){

    DBI::dbExecute(.con,glue::glue("USE {schema_name};"))

  }else{

    cli::cli_abort("
                   {.pkg {schema_name}} is not valid,
                   Use {.fn list_schemas} to list valid schemas.
                   Valid Schemas in  {.pkg {database_name}} are {.val {schema_valid_vec}}
                   ")
    }
  }

  cli::cli_h1("Status:")
  cli_show_user(.con)
  cli_show_db(.con)


}




#' @title Summarize a Lazy DBI Table
#' @name summary
#'
#' @description
#' The `summary.tbl_lazy()` method provides a database-aware summary interface
#' for lazy tables created via **dbplyr**. Instead of collecting data into R,
#' it constructs a SQL `SUMMARIZE` query and executes it remotely, returning
#' another lazy table reference.
#'
#' @param object A [`tbl_lazy`][dbplyr::tbl_lazy] object representing a remote
#'   database table or query.
#' @param ... Additional arguments (currently unused). Present for S3 method
#'   compatibility.
#'
#' @returns
#' A [`tbl_lazy`][dbplyr::tbl_lazy] object containing the summarized results,
#' still backed by the remote database connection.
#'
#' @details
#' This method does **not** pull data into memory. Instead, it creates a new
#' lazy query object representing the database-side summary. To retrieve the
#' summarized data, use `collect()` on the returned object.
#'
#' @examples
#' \dontrun{
#' library(DBI)
#' library(duckdb)
#' library(dplyr)
#'
#' con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
#' dbWriteTable(con, "mtcars", mtcars)
#'
#' tbl_obj <- tbl(con, "mtcars")
#'
#' # Returns a lazy summary table
#' summary(tbl_obj)
#'
#' dbDisconnect(con)
#' }
#'
#' @seealso
#' [dplyr::tbl()], [dplyr::collect()], [validate_con()]
#'
#' @export
summary.tbl_lazy <- function(object, ...){

  con <- dbplyr::remote_con(object)

  ## assert connection

  validate_con(con)

  query <- dbplyr::remote_query(object)

  summary_query <- paste0("summarize (",query,")")

  out <- dplyr::tbl(con,dplyr::sql(summary_query))

  return(out)
}





#' @title List Database Settings
#' @name list_setting
#'
#' @description
#' The `list_setting()` function provides a convenient way to inspect the
#' active configuration of a DuckDB or MotherDuck connection. It executes
#' the internal DuckDB function `duckdb_settings()` and returns the results
#' as a tidy tibble for easy viewing or filtering.
#'
#' @details
#' This function is particularly useful for debugging or auditing runtime
#' environments. All settings are returned as character columns, including
#' their names, current values, and default values.
#'
#' @inheritParams validate_con
#'
#' @returns
#' A [tibble] containing one row per setting with columns
#' describing the setting name, current value, description, and default value.
#'
#' @examples
#' \dontrun{
#' # Connect to DuckDB
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
#'
#' # List all database settings
#' list_setting(con)
#'
#' # Disconnect
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso
#' [list_shares]
#'
#' @export
list_setting <- function(.con){

  out <- DBI::dbGetQuery(
    .con
    ,"
  SELECT *
  FROM duckdb_settings()
  "
  ) |>
    dplyr::as_tibble()

  return(out)

}




#' @title List MotherDuck Shares
#' @name list_shares
#'
#' @description
#' The `list_shares()` function provides a convenient wrapper around the
#' MotherDuck SQL command `LIST SHARES;`. It validates that the supplied
#' connection is an active MotherDuck connection before executing the query.
#' If the connection is not valid, the function returns `0` instead of a table.
#'
#' @details
#' MotherDuck supports object sharing, which allows users to list and access
#' data shared between accounts. This function helps programmatically inspect
#' available shares within an authenticated MotherDuck session.
#' @inheritParams validate_con
#' @returns
#' A [tibble] containing details of available shares if
#' the connection is an MD connection or an empty tibble if not
#'
#' @examples
#' \dontrun{
#' # Connect to MotherDuck
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "md:")
#'
#' # List shares
#' list_shares(con)
#'
#' # Disconnect
#' DBI::dbDisconnect(con)
#' }
#'
#' @seealso [list_setting]
#'
#' @export
list_shares <- function(.con){


  valid_md <- validate_md_connection_status(.con,return_type = "arg")

  if(valid_md){

  out <- DBI::dbGetQuery(.con,"LIST SHARES;") |>
    dplyr::as_tibble()
  }else{
    out <- tibble::tibble()
  }

  return(out)

}


#' @importFrom contoso launch_ui
#' @export
contoso::launch_ui




utils::globalVariables(c("con", "extension_name", "installed", "loaded","current_role"))
