#' DuckDB CSV read configuration (config_csv)
#'
#' @description
#' A named character list of DuckDB CSV reading configuration settings used
#' by the package. This includes options such as type detection, delimiter,
#' quote handling, sample size, and other parser flags. These reflect the
#' values returned by `md::config_csv`.
#'
#' @format A named character list. Example names include:
#' \describe{
#'   \item{all_varchar}{character; "true"/"false"}
#'   \item{allow_quoted_nulls}{character; "true"/"false"}
#'   \item{auto_detect}{character; "true"/"false"}
#'   \item{auto_type_candidates}{character; types used for detection}
#'   \item{buffer_size}{character; buffer size in bytes}
#'   \item{columns}{character; column names/types struct or empty}
#'   \item{delim}{character; delimiter string}
#'   \item{skip}{character; number of lines to skip}
#' }
#' @examples
#' \dontrun{
#' config_csv$all_varchar
#' }
#'
#' @docType data
"config_csv"


#' DuckDB Excel read configuration (config_excel)
#'
#' @description
#' A named character list of DuckDB Excel reading configuration settings
#' used by the package. Includes options such as reading binary as string,
#' adding filename/row_number columns, Hive partitioning, and union by name.
#' These reflect the values returned by `md::config_excel`.
#'
#' @format A named character list. Example names include:
#' \describe{
#'   \item{binary_as_string}{character; "true"/"false"}
#'   \item{encryption_config}{character; encryption struct or "-" if none}
#'   \item{filename}{character; "true"/"false"}
#'   \item{file_row_number}{character; "true"/"false"}
#'   \item{hive_partitioning}{character; e.g., "(auto-detect)"}
#'   \item{union_by_name}{character; "true"/"false"}
#' }
#' @examples
#' \dontrun{
#' config_excel$binary_as_string
#' }
#' @docType data
"config_excel"

#' DuckDB Parquet read configuration (config_parquet)
#'
#' @description
#' A named character list of DuckDB Parquet reading configuration settings
#' used by the package. Includes options such as binary encoding, filename
#' columns, row numbers, and union by name. Reflects `md::config_parquet`.
#'
#' @format A named character list. Example names include:
#' \describe{
#'   \item{binary_as_string}{character; "true"/"false"}
#'   \item{encryption_config}{character; encryption struct or "-" if none}
#'   \item{filename}{character; "true"/"false"}
#'   \item{file_row_number}{character; "true"/"false"}
#'   \item{hive_partitioning}{character; e.g., "(auto-detect)"}
#'   \item{union_by_name}{character; "true"/"false"}
#' }
#' @examples
#' \dontrun{
#' config_parquet$binary_as_string
#' }
#' @docType data
"config_parquet"


#' DuckDB runtime database configuration (config_db)
#'
#' @description
#' A named character list of DuckDB runtime configuration settings used by the package.
#' This object includes allocator settings, thread counts, extension flags, storage and checkpoint options,
#' security and secret settings, and other engine-level options. These settings reflect the values returned by
#' `md::config_db` and are suitable as a template or reference for configuring DuckDB instances created via the package.
#'
#' @format A named character list. Example names and values include:
#' \describe{
#'   \item{access_mode}{character; e.g. "automatic"}
#'   \item{allocator_background_threads}{character; "true"/"false"}
#'   \item{allocator_bulk_deallocation_flush_threshold}{character; e.g. "512MB"}
#'   \item{allocator_flush_threshold}{character; e.g. "128MB"}
#'   \item{allow_community_extensions}{character; "true"/"false"}
#'   \item{allow_extensions_metadata_mismatch}{character; "true"/"false"}
#'   \item{allow_persistent_secrets}{character; "true"/"false"}
#'   \item{allow_unredacted_secrets}{character; "true"/"false"}
#'   \item{allow_unsigned_extensions}{character; "true"/"false"}
#'   \item{arrow_large_buffer_size}{character; "true"/"false"}
#'   \item{arrow_lossless_conversion}{character; "true"/"false"}
#'   \item{arrow_output_list_view}{character; "true"/"false"}
#'   \item{autoinstall_extension_repository}{character; URL or empty string}
#'   \item{autoinstall_known_extensions}{character; "true"/"false"}
#'   \item{autoload_known_extensions}{character; "true"/"false"}
#'   \item{ca_cert_file}{character; path to certificate file, or empty string}
#'   \item{catalog_error_max_schemas}{character; numeric as string, e.g. "100"}
#'   \item{checkpoint_threshold}{character; e.g. "16MB"}
#'   \item{wal_autocheckpoint}{character; e.g. "16MB"}
#'   \item{custom_extension_repository}{character; URL or empty string}
#'   \item{custom_user_agent}{character; string or empty}
#'   \item{default_block_size}{character; e.g. "262144"}
#'   \item{default_collation}{character; collation name or empty}
#'   \item{default_null_order}{character; e.g. "NULLS_LAST"}
#'   \item{null_order}{character; e.g. "NULLS_LAST"}
#'   \item{default_order}{character; e.g. "ASC"}
#'   \item{default_secret_storage}{character; e.g. "local_file"}
#'   \item{disabled_compression_methods}{character; list or empty}
#'   \item{duckdb_api}{character; e.g. "cli"}
#'   \item{enable_external_access}{character; "true"/"false"}
#'   \item{enable_external_file_cache}{character; "true"/"false"}
#'   \item{enable_fsst_vectors}{character; "true"/"false"}
#'   \item{enable_http_metadata_cache}{character; "true"/"false"}
#'   \item{enable_macro_dependencies}{character; "true"/"false"}
#'   \item{enable_object_cache}{character; "true"/"false"}
#'   \item{enable_server_cert_verification}{character; "true"/"false"}
#'   \item{enable_view_dependencies}{character; "true"/"false"}
#'   \item{extension_directory}{character; path or empty}
#'   \item{external_threads}{character; numeric as string, e.g. "1"}
#'   \item{force_download}{character; "true"/"false"}
#'   \item{immediate_transaction_mode}{character; "true"/"false"}
#'   \item{index_scan_max_count}{character; numeric as string, e.g. "2048"}
#'   \item{index_scan_percentage}{character; numeric as string, e.g. "0.001"}
#'   \item{lock_configuration}{character; "true"/"false"}
#'   \item{max_vacuum_tasks}{character; numeric as string, e.g. "100"}
#'   \item{old_implicit_casting}{character; "true"/"false"}
#'   \item{parquet_metadata_cache}{character; "true"/"false"}
#'   \item{preserve_insertion_order}{character; "true"/"false"}
#'   \item{produce_arrow_string_view}{character; "true"/"false"}
#'   \item{scheduler_process_partial}{character; "true"/"false"}
#'   \item{secret_directory}{character; path for persistent secrets}
#'   \item{storage_compatibility_version}{character; e.g. "v0.10.2"}
#'   \item{temp_directory}{character; path or empty string}
#'   \item{threads}{character; number of threads as string, e.g. "4"}
#'   \item{worker_threads}{character; number of threads as string, e.g. "4"}
#'   \item{username}{character; string or empty}
#'   \item{user}{character; string or empty}
#'   \item{zstd_min_string_length}{character; numeric as string, e.g. "4096"}
#' }
#'
#' @examples
#' # inspect the config
#' md::config_db
#' # access individual settings
#' md::config_db$threads
#'
#' @docType data
"config_db"
