#' @title Show current database user
#' @name show_current_user
#' @inheritParams validate_con
#' @inheritParams connect_to_motherduck
#'
#' @description
#' Return or print the current database user for a MotherDuck / DuckDB connection.
#'
#' @details
#' This helper queries the active DB connection for the current user (via
#' `SELECT current_user`). You may either provide an existing DBI connection
#' via `.con` or provide a `motherduck_token` and let the function open a
#' short-lived connection for you. When the function opens a connection it
#' will close it before returning.
#'
#' The function supports two output modes:
#' * `"msg"` — prints a small informative message and returns the result
#'   invisibly (useful for interactive use),
#' * `"arg"` — returns a tibble containing the `current_user` column.
#'
#' @param return Character scalar, one of `"msg"` or `"arg"`. Default: `"msg"`.
#'
#' @examples
#' \dontrun{
#' # Using an existing connection
#' con <- connect_to_motherduck("my_token")
#' show_current_user(.con = con, return = "msg")
#'
#' # Let the function open a connection from a token
#' tbl <- show_current_user(motherduck_token = "my_token", return = "arg")
#' }
#'
#' @export

show_current_user <- function(.con,motherduck_token,return="msg"){

    return_valid_vec <- c("msg","arg")

    rlang::arg_match(
        return
        ,values = c("msg","arg")
        ,multiple = FALSE
        ,error_arg ="return"
    )

    if(!missing(.con)){

      validate_md_connection_status(.con)

    }

    if(!missing(motherduck_token)){

    .con <-   connect_to_motherduck(motherduck_token=motherduck_token)

    }

    current_user_tbl <- DBI::dbGetQuery(.con,"select current_user") |>
    tibble::as_tibble()

    if(return=="msg"){

    cli::cli_alert("FYI Your current user name is {cli::col_br_red(current_user_tbl$current_user)}")

    }

    if(return=="arg"){

    return(current_user_tbl)

    }

}


#' @title Check HTTP response status and format JSON output
#' @name check_resp_status_and_tidy_response
#'
#' @description
#' Validates that an HTTP response succeeded (status code `200`) and converts
#' a JSON API response into a tidy tibble with user-specified column names.
#'
#' @details
#' This function checks the status code from an API response object.
#' If the response is successful (`status_code == 200`):
#' - If the response payload is empty, the full JSON object is returned as-is.
#' - Otherwise, the function flattens the JSON response and tidies it into
#'   a tibble with two columns corresponding to `column_name1` and `column_name2`.
#'
#' @param resp An HTTP response object, typically returned by a request made
#'   with `httr` or `httr2`, containing a `status_code` field.
#' @param json_response A parsed JSON object (e.g., from `httr::content()` or
#'   `jsonlite::fromJSON()`), representing the API response body.
#' @param column_name1 A character scalar specifying the name of the first column
#'   to assign to the tidied tibble.
#' @param column_name2 A character scalar specifying the name of the second column
#'   to assign to the tidied tibble.
#'
#' @return
#' A tibble containing the tidied response if the request succeeded, or the
#' original JSON object if the payload is empty.
#'
#' @examples
#' \dontrun{
#' resp <- httr::GET("https://api.example.com/data")
#' json_resp <- httr::content(resp)
#' check_resp_status_and_tidy_response(
#'   resp,
#'   json_response = json_resp,
#'   column_name1 = "key",
#'   column_name2 = "value"
#' )
#' }
#'
#' @keywords internal
#' @noRd
check_resp_status_and_tidy_response <- function(resp,json_response,column_name1,column_name2){


    # column_name1="test1"
    # column_name2="test2"

    if (all(resp$status_code == 200)){

        # If the 'accounts' field in the response is empty, return the whole JSON response
        if (all(json_response |> purrr::pluck(1) |> length()==0)) {

            return(json_response)
        }

        # Otherwise, transform the response into a tibble with two columns:

        out <- json_response |>
            unlist() |>  # Flatten the list
            utils::stack() |>  # Convert to data frame
            tibble::as_tibble() |>  # Convert to tibble
            dplyr::select(
                !!column_name1:= ind,
                !!column_name2:= values
            )

        # Return the formatted output
        return(out)
    }

}


#' @title Validate MotherDuck token from environment
#' @name validate_motherduck_token_env
#'
#' @description
#' Internal helper that retrieves and validates the MotherDuck authentication
#' token from the system environment.
#'
#' @details
#' This function checks whether an environment variable (default:
#' `"MOTHERDUCK_TOKEN"`) exists and contains a non-empty value.
#' If so, the value of that environment variable is returned.
#' Otherwise, the original `motherduck_token` argument is returned unchanged.
#'
#' @param motherduck_token A character scalar giving the name of the environment
#'   variable that stores the MotherDuck token. Defaults to `"MOTHERDUCK_TOKEN"`.
#'
#' @return
#' A character string representing the resolved MotherDuck token.
#'
#' @keywords internal
#' @noRd
validate_motherduck_token_env <- function(motherduck_token="MOTHERDUCK_TOKEN"){

    assertthat::assert_that(
        is.character(motherduck_token)
    )

    motherduck_token_env <- Sys.getenv(motherduck_token)


    # If the environment variable is not empty, override 'motherduck_token' with its value
    if (!nchar(motherduck_token_env) == 0) {
        motherduck_token = motherduck_token_env
    }

    return(motherduck_token)
}


#' @title List active MotherDuck accounts
#' @name list_md_active_accounts
#'
#' @description
#' Retrieves a list of active MotherDuck accounts available to the authenticated
#' user, returning the results as a tidy tibble.
#'
#' @details
#' This function queries the MotherDuck REST API endpoint
#' (`https://api.motherduck.com/v1/active_accounts`) using the provided or
#' environment-resolved authentication token.
#' The API response is validated and converted into a two-column tibble using
#' [check_resp_status_and_tidy_response()], where:
#' - `account_settings` contains the configuration keys.
#' - `account_values` contains their corresponding values.
#'
#' If `motherduck_token` is not explicitly provided, the function attempts to
#' resolve it from the `MOTHERDUCK_TOKEN` environment variable using
#' [validate_motherduck_token_env()].
#' The current user name is also displayed via [show_current_user()].
#'
#' @inheritParams connect_to_motherduck
#'
#' @return
#' A tibble with two columns:
#' - `account_settings`: configuration keys for the active accounts.
#' - `account_values`: corresponding configuration values.
#'
#' @examples
#' \dontrun{
#' # Retrieve active accounts for the authenticated user
#' accounts_tbl <- list_md_active_accounts()
#' print(accounts_tbl)
#' }
#'
#' @export

list_md_active_accounts <- function(motherduck_token="MOTHERDUCK_TOKEN"){

    # https://motherduck.com/docs/sql-reference/rest-api/ducklings-get-duckling-config-for-user/

    motherduck_token_env=validate_motherduck_token_env(motherduck_token)

    show_current_user(motherduck_token = motherduck_token)

    # Make a GET request to the MotherDuck API to retrieve active accounts
    resp <- httr2::request("https://api.motherduck.com/v1/active_accounts") |>
        httr2::req_headers(
            "Accept" = "application/json",  # Request JSON response
            "Authorization" = paste("Bearer", motherduck_token_env)  # Add auth token to header
        ) |>
        httr2::req_error(is_error = function(resp) FALSE) |>
        httr2::req_perform()  # Perform the HTTP request

    # Parse the JSON response body
    json_response <- httr2::resp_body_json(resp)

    out <- check_resp_status_and_tidy_response(
        resp = resp
        ,json_response = json_response
        ,column_name1 = "account_settings"
        ,column_name2 = "account_values"
        )


    return(out)
}




#' @title List a MotherDuck user's tokens
#' @name list_md_user_tokens
#'
#' @description
#' Retrieves all active authentication tokens associated with a specific
#' MotherDuck user account, returning them as a tidy tibble.
#'
#' @details
#' This function queries the MotherDuck REST API endpoint
#' `https://api.motherduck.com/v1/users/{user_name}/tokens` to list the tokens
#' available for the specified user.
#'
#' It uses the provided or environment-resolved `motherduck_token` for
#' authorization. If `motherduck_token` is not explicitly provided, the function
#' attempts to resolve it from the `MOTHERDUCK_TOKEN` environment variable via
#' [validate_motherduck_token_env()].
#' The current authenticated user is displayed via [show_current_user()] for
#' verification.
#' The resulting JSON response is validated and tidied into a two-column tibble
#' using [check_resp_status_and_tidy_response()], with token attributes and their
#' corresponding values.
#'
#' @param user_name A character string specifying the MotherDuck user name whose
#'   tokens should be listed.
#' @inheritParams connect_to_motherduck
#'
#' @return
#' A tibble with two columns:
#' - `token_settings`: metadata fields associated with each token.
#' - `token_values`: corresponding values for those fields.
#'
#' @examples
#' \dontrun{
#' # List tokens for a specific user
#' tokens_tbl <- list_md_user_tokens(user_name = "alejandro_hagan")
#' print(tokens_tbl)
#' }
#'
#' @seealso
#' [list_md_active_accounts()], [validate_motherduck_token_env()],
#' [check_resp_status_and_tidy_response()], [show_current_user()]
#'
#' @export
list_md_user_tokens <- function(user_name,motherduck_token="MOTHERDUCK_TOKEN"){


    #https://motherduck.com/docs/sql-reference/rest-api/users-list-tokens/

    # test
    # user_name="alejandro_hagan"
    # motherduck_token="MOTHERDUCK_TOKEN"

    show_current_user(motherduck_token = motherduck_token,return = "msg")

    motherduck_token <- validate_motherduck_token_env(motherduck_token)

    resp <- httr2::request(paste0("https://api.motherduck.com/v1/users/",user_name,"/tokens")) |>
        httr2::req_headers(
            "Accept" = "application/json",
            "Authorization" = paste("Bearer",motherduck_token)
        ) |>
        httr2::req_error(is_error = function(resp) FALSE) |>
        httr2::req_perform()

    # Parse the response JSON
    json_response <- httr2::resp_body_json(resp)

    out <- check_resp_status_and_tidy_response(resp,json_response,column_name1 = "token_settings",column_name2="token_values")

    return(out)

}


#' @title List a MotherDuck user's instance settings
#' @name list_md_user_instance
#'
#' @description
#' Retrieves configuration and instance-level settings for a specified
#' MotherDuck user, returning the results as a tidy tibble.
#'
#' @details
#' This function calls the MotherDuck REST API endpoint
#' `https://api.motherduck.com/v1/users/{user_name}/instances` to fetch
#' information about the user’s active DuckDB instances and their configuration
#' parameters.
#'
#' It uses the provided `motherduck_token` or attempts to resolve it from the
#' `MOTHERDUCK_TOKEN` environment variable via
#' [validate_motherduck_token_env()].
#' The current authenticated user is displayed with [show_current_user()] for
#' verification.
#' The API response is validated and tidied into a two-column tibble using
#' [check_resp_status_and_tidy_response()], containing instance descriptions and
#' their corresponding values.
#'
#' @inheritParams list_md_user_tokens
#' @return
#' A tibble with two columns:
#' - `instance_desc`: names or descriptions of instance configuration settings.
#' - `instance_values`: corresponding values for each configuration field.
#'
#' @examples
#' \dontrun{
#' # List instance settings for a specific user
#' instance_tbl <- list_md_user_instance(user_name ="Bob Smith")
#' }
#'
#' @seealso
#' [list_md_user_tokens()], [list_md_active_accounts()],
#' [validate_motherduck_token_env()], [check_resp_status_and_tidy_response()],
#' [show_current_user()]
#'
#' @export
list_md_user_instance <- function(user_name,motherduck_token="MOTHERDUCK_TOKEN"){

    #https://motherduck.com/docs/sql-reference/rest-api/ducklings-get-duckling-config-for-user/

    # user_name <- "alejandro_hagan"
    motherduck_token_env <- validate_motherduck_token_env(motherduck_token)

    show_current_user(motherduck_token_env)

    resp <- httr2::request(paste0("https://api.motherduck.com/v1/users/",user_name,"/instances")) |>
        httr2::req_headers(
            "Accept" = "application/json",
            "Authorization" = paste("Bearer", motherduck_token)
        ) |>
        httr2::req_error(is_error = function(resp) FALSE) |>
        httr2::req_perform()

    # Parse JSON response
    json_response <- httr2::resp_body_json(resp)

    out <- check_resp_status_and_tidy_response(resp = resp,json_response = json_response,column_name1 = "instance_desc",column_name2 = "instance_values")

    return(out)

}

#' @title Delete a MotherDuck user
#'
#' @name delete_md_user
#'
#' @description
#' Sends a `DELETE` request to the MotherDuck REST API to permanently remove a user
#' from your organization. This operation requires administrative privileges and a
#' valid MotherDuck access token.
#'
#' @inheritParams list_md_user_tokens
#'
#' @details
#' This function calls the
#' [MotherDuck Users API](https://motherduck.com/docs/sql-reference/rest-api/users-delete/)
#' endpoint to delete the specified user. The authenticated user (associated with the
#' provided token) must have sufficient permissions to perform user management actions.
#'
#' @return
#' A tibble summarizing the API response, including the username and deletion status.
#'
#' @seealso
#' [create_md_user()] for creating new users and
#' [list_md_user_tokens()] for listing user tokens.
#'
#' @examples
#' \dontrun{
#' # Delete a user named "bob_smith" using an admin token stored in an environment variable
#' delete_md_user("bob_smith", "MOTHERDUCK_TOKEN")
#' }
#'
#' @export
delete_md_user <- function(user_name, motherduck_token = "MOTHERDUCK_TOKEN") {
  # Validate inputs
  assertthat::assert_that(is.character(user_name), length(user_name) == 1)
  assertthat::assert_that(is.character(motherduck_token), length(motherduck_token) == 1)

  # Resolve environment token if needed
  motherduck_token_env <- validate_motherduck_token_env(motherduck_token)

  # Optionally show the current user (informative only)
  show_current_user(motherduck_token = motherduck_token_env, return = "msg")

  # Build and send DELETE request
  resp <- httr2::request(paste0("https://api.motherduck.com/v1/users/", user_name)) |>
    httr2::req_method("DELETE") |>
    httr2::req_headers(
      Accept = "application/json",
      Authorization = paste("Bearer", motherduck_token_env)
    ) |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_perform()

  # Parse JSON response
  json_response <- httr2::resp_body_json(resp)

  # Tidy up output
  out <- check_resp_status_and_tidy_response(
    resp = resp,
    json_response = json_response,
    column_name1 = "username",
    column_name2 = "value"
  )

  return(out)
}





#' @title Create a new MotherDuck user
#'
#' @name create_md_user
#'
#' @description
#' Sends a `POST` request to the MotherDuck REST API to create a new user
#' within your organization. This operation requires administrative privileges
#' and a valid access token.
#'
#' @details
#' This function calls the
#' [MotherDuck Users API](https://motherduck.com/docs/sql-reference/rest-api/users-create/)
#' endpoint to create a new user under the authenticated account.
#' The provided token must belong to a user with permissions to manage
#' organization-level accounts.
#'@inheritParams list_md_user_tokens
#' @return
#' A tibble summarizing the API response, typically containing the newly created
#' username and associated metadata.
#'
#' @seealso
#' [delete_md_user()] for deleting users, and
#' [list_md_user_tokens()] for listing tokens associated with a given user.
#'
#' @examples
#' \dontrun{
#' # Create a new user in MotherDuck using an admin token stored in an environment variable
#' create_md_user("test_20250913", "MOTHERDUCK_TOKEN")
#' }
#'
#' @export

create_md_user <- function(user_name, motherduck_token = "MOTHERDUCK_TOKEN") {
  # Validate inputs
  assertthat::assert_that(
    is.character(user_name),
    length(user_name) == 1,
    !is.na(user_name)
  )

  # Resolve environment token if needed
  motherduck_token_env <- validate_motherduck_token_env(motherduck_token)

  # Optionally show current user (for logging)
  show_current_user(motherduck_token = motherduck_token_env, return = "msg")

  # Build and send POST request
  resp <- httr2::request("https://api.motherduck.com/v1/users") |>
    httr2::req_method("POST") |>
    httr2::req_headers(
      "Content-Type" = "application/json",
      "Accept" = "application/json",
      "Authorization" = paste("Bearer", motherduck_token_env)
    ) |>
    httr2::req_body_json(list(username = user_name)) |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_perform()

  # Parse JSON response
  json_response <- httr2::resp_body_json(resp)

  # Clean up and return output
  out <- check_resp_status_and_tidy_response(
    resp = resp,
    json_response = json_response,
    column_name1 = "username",
    column_name2 = "value"
  )

  return(out)
}


#' @title Create a MotherDuck access token
#'
#' @name create_md_access_token
#'
#' @description
#' Creates a new access token for a specified MotherDuck user using the REST API.
#' Tokens can be configured with a specific type, name, and expiration time.
#' @inheritParams list_md_user_tokens
#' @param token_type Character. The type of token to create. Must be one of:
#'   `"read_write"` or `"read_scaling"`.
#' @param token_name Character. A descriptive name for the token.
#' @param token_expiration_number Numeric. The duration of the token’s validity,
#'   in the units specified by `token_expiration_unit`. Minimum value is 300 seconds.
#' @param token_expiration_unit Character. The unit of time for the token expiration.
#'   One of `"seconds"`, `"minutes"`, `"days"`, `"weeks"`, `"months"`, `"years"`, or `"never"`.
#'
#' @details
#' This function calls the MotherDuck REST API endpoint
#' `https://api.motherduck.com/v1/users/{user_name}/tokens` to create a new token
#' for the specified user. The token’s time-to-live (TTL) is calculated in seconds
#' from `token_expiration_number` and `token_expiration_unit`.
#' The authenticated user must have administrative privileges to create tokens.
#'
#' @return
#' A tibble containing the API response, including the username and the token
#' attributes.
#'
#' @seealso
#' [list_md_user_tokens()] for retrieving tokens of a user, and
#' [list_md_active_accounts()] for listing available accounts.
#'
#' @examples
#' \dontrun{
#' # Create a temporary read/write token for user "alejandro_hagan" valid for 1 hour
#' create_md_access_token(
#'   user_name = "alejandro_hagan",
#'   token_type = "read_write",
#'   token_name = "temp_token",
#'   token_expiration_number = 1,
#'   token_expiration_unit = "hours",
#'   motherduck_token = "MOTHERDUCK_TOKEN"
#' )
#' }
#'
#' @export
create_md_access_token <- function(user_name,token_type,token_name,token_expiration_number,token_expiration_unit,motherduck_token="MOTHERDUCK_TOKEN"){

    # test inputs
    # user_name <- "alejandro_hagan"
    # token_type <- "read_write"
    # token_expiration_number=300
    # token_expiration_unit="second"
    # token_name <- "temp"

    valid_token_type_vec <- c("read_write", "read_scaling")


    rlang::arg_match(
        token_type
        ,valid_token_type_vec
        ,multiple = FALSE
        ,error_arg = cli::format_error("Please select {.or {valid_token_type_vec}} instead of {token_type}")
    )

    seconds_vec <- convert_to_seconds(number = token_expiration_number,units = token_expiration_unit)

    # Replace these with your actual values
    validate_motherduck_token_env <- validate_motherduck_token_env(motherduck_token="MOTHERDUCK_TOKEN")

    assertthat::assert_that(
        is.character(user_name)
        ,is.character(token_name)
    )

    active_accounts_tbl <- list_md_active_accounts(motherduck_token = motherduck_token)


    # account_values_vec <- active_accounts_tbl |>
    #     dplyr::filter(
    #         account_settings=="accounts.username"
    #     ) |> pull(account_values)
    #
    #
    # assertthat::assert_that(
    #     any(user_name %in% account_values_vec)
    # )

    # Construct the URL
    url <- paste0("https://api.motherduck.com/v1/users/",user_name,"/tokens")

    # Create and send the request
    resp <- httr2::request(url) |>
        httr2::req_method("POST") |>
        httr2::req_headers(
            "Content-Type" = "application/json",
            "Accept" = "application/json",
            "Authorization" = paste("Bearer",validate_motherduck_token_env)
        ) |>
        httr2::req_body_json(
            list(
                ttl = seconds_vec
                ,name = token_name
                ,token_type = token_type
                )
            ) |>
        httr2::req_error(is_error = function(resp) FALSE) |>
        httr2::req_perform()

    # Parse the JSON response
    json_response <- httr2::resp_body_json(resp)

    out <- check_resp_status_and_tidy_response(
        resp = resp
        ,json_response =json_response
        ,column_name1 = "username"
        ,column_name2 = "value"
    )


    return(out)


}





#' @title Delete a MotherDuck user's access token
#'
#' @name delete_md_access_token
#'
#' @description
#' Deletes a specific access token for a given MotherDuck user using the REST API.
#' This operation requires administrative privileges and a valid API token.
#' @inheritParams list_md_user_tokens
#' @param token_name Character. The name of the access token to delete.
#'
#' @details
#' This function calls the MotherDuck REST API endpoint
#' `https://api.motherduck.com/v1/users/{user_name}/tokens/{token_name}`
#' using a `DELETE` request to remove the specified token.
#' The authenticated user must have sufficient permissions to perform token management.
#'
#' @return
#' A tibble summarizing the API response, typically including the username and
#' deletion status of the token.
#'
#' @seealso
#' [create_md_access_token()] for creating new tokens, and
#' [list_md_user_tokens()] for listing existing tokens.
#'
#' @examples
#' \dontrun{
#' # Delete a token named "temp_token" for user "alejandro_hagan"
#' delete_md_access_token(
#'   user_name = "alejandro_hagan",
#'   token_name = "temp_token",
#'   motherduck_token = "MOTHERDUCK_TOKEN"
#' )
#' }
#'
#' @export
delete_md_access_token <- function(user_name,token_name,motherduck_token="MOTHERDUCK_TOKEN"){

    motherduck_token_env <- validate_motherduck_token_env(motherduck_token)

    url <- paste0("https://api.motherduck.com/v1/users/", user_name, "/tokens/", motherduck_token_env)

    # Create the request
    resp <- httr2::request(url) |>
        httr2::req_method("DELETE") |>
        httr2::req_headers(
            "Accept" = "application/json",
            "Authorization" = paste("Bearer", motherduck_token_env)
        ) |>
        httr2::req_error(is_error = function(resp) FALSE) |>  # Optional: prevent automatic errors
        httr2::req_perform()

    json_response <- httr2::resp_body_json(resp)

    out <- check_resp_status_and_tidy_response(
        resp = resp
        ,json_response =json_response
        ,column_name1 = "username"
        ,column_name2 = "value"
    )

}


#' @title Configure motherduck user's settings
#' @name configure_md_user_settings
#' @param user_name motherduck user name
#' @param motherduck_token user's access token
#' @param token_type the to be token type
#' @param instance_size the to be instance size
#' @param flock_size the to be flock size
#'
#' @returns tibble
#' @export
#'
configure_md_user_settings <- function(
        user_name
        ,motherduck_token="MOTHERDUCK_TOKEN"
        ,token_type="read_write"
        ,instance_size="pulse"
        ,flock_size=0
        ){

    assertthat::assert_that(
        is.character(user_name)
        ,is.numeric(flock_size)
    )


    token_type_vec <- validate_token_type(token_type)

    motherduck_token_env <- validate_motherduck_token_env(motherduck_token)

    # URL
    url <- paste0("https://api.motherduck.com/v1/users/", user_name, "/instances")

    # Request body
    body <- list(
        config = list(
            token_type_vec = list(
                instance_size =instance_size
            )
        )
    )

    # Make the PUT request
    resp <- httr2::request(url) |>
        httr2::req_method("PUT") |>
        httr2::req_headers(
            "Content-Type" = "application/json",
            "Accept" = "application/json",
            "Authorization" = paste("Bearer", motherduck_token_env)
        ) |>
        httr2::req_body_json(body) |>
        httr2::req_error(is_error = function(resp) FALSE) |>  # Optional: handle errors manually
        httr2::req_perform()


    json_response <- httr2::resp_body_json(resp)

    out <- check_resp_status_and_tidy_response(
        resp = resp
        ,json_response =json_response
        ,column_name1 = "username"
        ,column_name2 = "value"
    )
}

#' @title Convert units to seconds
#' @name convert_to_seconds
#' @param number numeriv value
#' @param units second, minute, day, month, year or never
#'
#' @returns number
#'
#' @examples
#' \dontrun{
#' convert_to_seconds(300,"days")
#' }
convert_to_seconds <- function(number,units){

    # units <- "day"
    # number <- 100

    valid_units <- c("seconds","second","minutes","minute","days","day","months","month","years","year","never")

    units <- tolower(units)

    assertthat::assert_that(
        length(units)==1
        ,length(number)==1
        ,is.character(units)
        ,is.numeric(number)
    )

    unit_vec <- rlang::arg_match(
        ,arg=units
        ,values = valid_units
        ,multiple = TRUE
    )


    conversion_factors_vec = c(
        "second"= 1,
        "seconds"=1,
        "minute"= 60,
        "minutes"=60,
        "day"= 86400,
        "days"= 86400,
        "month"= 2592000,
        "months"= 2592000,
        "year"= 31536000,
        "years"= 31536000
    )

    if(unit_vec=="never"){
        out <- NA
        return(out)
    }

   seconds <-  conversion_factors_vec[unit_vec]*number
   out <- unname(seconds)

   return(out)

}


#' @title Validate MD token type args
#' @name validate_token_type
#' @param token_type character vector either read_write or read_scaling
#'
#' @returns vector
#'
validate_token_type <- function(token_type){


    token_type_vec <- rlang::arg_match(
        token_type
        ,values = c("read_write", "read_scaling")
        ,multiple = FALSE
        ,error_arg = "token_type"
    )

    return(token_type_vec)

}

#' @title Validate instance size args
#' @name validate_instance_size
#' @param instance_size select either "pulse", "standard", "jumbo", "mega", "giga"
#'
#' @returns character vector
#'
validate_instance_size <- function(instance_size){


    instance_size_vec <- rlang::arg_match(
        tolower(instance_size)
        ,values = c("pulse", "standard", "jumbo", "mega", "giga")
        ,multiple = FALSE
        ,error_arg = "instance_size"
    )

    return(instance_size_vec)

}

#' @title Validate flock size args
#' @name validate_flock_size
#' @param flock_size whole number between 0-60
#'
#' @returns integer
#'
validate_flock_size <- function(flock_size){


    flock_size_int <- rlang::as_integer(flock_size)

    assertthat::assert_that(
        length(flock_size_int)==1
        ,is.integer(flock_size_int)
        ,flock_size_int %in% c(0:60)
        ,env = cli::cli_abort("Enter a single whole number between 0-60. You entered {flock_size}")
    )


    return(flock_size_int)

}



#' @@title Create or replace a motherduck share
#'
#' @param .con md connection
#' @param share_name shame name
#' @param database_name database name to be shared
#' @param access either "RESTRICTED" or "PUBLIC"
#' @param visibility either "HIDDEN"or  "LISTED"
#' @param update either "AUTOMATIC" or "MANUAL"
#'
#' @returns message
#' @export
#'
create_or_replace_share <- function(.con,
                                    share_name,
                                    database_name,
                                    access        =  "PUBLIC",
                                    visibility    = "LISTED",
                                    update        =  "AUTOMATIC") {
    # Validate arguments

    valid_access_vec = c("RESTRICTED", "PUBLIC")
    valid_visibility_vec = c("HIDDEN", "LISTED")
    valid_update_vec = c("AUTOMATIC", "MANUAL")

    assertthat::assert_that(
        is.character(share_name)
        ,is.character(database_name)
    )

    rlang::arg_match(
        access
        ,values = valid_access_vec
        ,multiple = FALSE
        ,error_arg = "access"
    )

    rlang::arg_match(
        visibility
        ,values = valid_visibility_vec
        ,multiple = FALSE
        ,error_arg = "visibility"
    )


    rlang::arg_match(
        update
        ,values = valid_visibility_vec
        ,multiple = FALSE
        ,error_arg = "update"
    )


    # Build SQL statement
    query <- glue::glue("
    CREATE OR REPLACE SHARE {share_name} FROM {database_name} (
      ACCESS {access},
      VISIBILITY {visibility},
      UPDATE {update}
    );
  ")

    show_current_user(.con=.con)
    # Execute query
    DBI::dbExecute(.con, query)
}



#' Create a MD share of a database
#' @name create_if_not_exists_share
#' @param .con MD connection
#' @param share_name new share name
#' @param database_name target database
#' @param access "RESTRICTED" or "PUBLIC"
#' @param visibility "HIDDEN" or "LISTED"
#' @param update "AUTOMATIC" or "MANUAL"
#'
#' @returns message
#' @export
#'
create_if_not_exists_share <- function(.con,
                         share_name,
                         database_name,
                         access        =  "PUBLIC",
                         visibility    = "LISTED",
                         update        =  "AUTOMATIC") {
    # Validate arguments

    valid_access_vec     = c("RESTRICTED", "PUBLIC")
    valid_visibility_vec = c("HIDDEN", "LISTED")
    valid_update_vec     = c("AUTOMATIC", "MANUAL")

    assertthat::assert_that(
        is.character(share_name)
        ,is.character(database_name)
    )

    rlang::arg_match(
        access
        ,values = valid_access_vec
        ,multiple = FALSE
        ,error_arg = "access"
    )

    rlang::arg_match(
        visibility
        ,values = valid_visibility_vec
        ,multiple = FALSE
        ,error_arg = "visibility"
    )


    rlang::arg_match(
        update
        ,values = valid_visibility_vec
        ,multiple = FALSE
        ,error_arg = "update"
    )

    validate_md_connection_status(.con,return_type = "arg")

    # Build SQL statement
   DBI::dbExecute(
   .con
   ,glue::glue_sql("
    CREATE IF NOT EXISTS {share_name} FROM {database_name} (
      ACCESS {access},
      VISIBILITY {visibility},
      UPDATE {update}
    );",.con=.con)
    )

    show_current_user(.con=.con)

}



#' @title Describe share
#' @name describe_share
#' @inheritParams validate_con
#' @param share_name shared path name
#'
#' @returns tibble
#' @export
#'
describe_share <- function(.con, share_name) {

    assertthat::assert_that(
        is.character(share_name)
    )


    # Build and run the query
    out <-DBI::dbGetQuery(.con,glue::glue_sql("SELECT * FROM md_describe_database_share('{share_name}');",.con=.con)) |>
      tibble::as_tibble()

    # Return the result as a data frame
    return(out)
}



#' @title Drop a MD share name
#' @name drop_share
#'
#' @inheritParams validate_con
#' @param share_name Share name
#'
#' @returns message
#' @export
#'
drop_share <- function(.con, share_name) {

  # .con <- con_md
  # share_name <- "test"
    #validate inputs
    validate_md_connection_status(.con,return_type = "arg")

    assertthat::assert_that(
        is.character(share_name)
    )

    # Sanitize share_name by wrapping with double quotes
    share_name_quoted <- DBI::dbQuoteIdentifier(.con, share_name)

    suppressWarnings(
    valid_share_name <- unique(list_shares(.con)$name)
    )

    if(any(share_name %in% valid_share_name)){

    DBI::dbExecute(.con,glue::glue_sql("DROP SHARE IF EXISTS {share_name_quoted};",.con=.con))

    }else{

      cli::cli_alert_warning("There is no share named {share_name}")

    }

    show_current_user(.con)

}


#' @title List all owned shares
#' @name list_owned_shares
#' @param .con motherduck connection
#'
#' @returns tibble
#' @export
#'
list_owned_shares <- function(.con) {

    DBI::dbGetQuery(.con, "LIST SHARES;") |>
        tibble::as_tibble()
}

#' @title List all shares that are shared with you
#' @name list_shared_with_me_shares
#' @inheritParams validate_con
#'
#' @returns tibble
#' @export
#'
list_shared_with_me_shares <- function(.con) {

    dplyr::tbl(.con, dplyr::sql("select * from MD_INFORMATION_SCHEMA.SHARED_WITH_ME;")) |>
        tibble::as_tibble()
}

utils::globalVariables(c(":=","ind","values"))
