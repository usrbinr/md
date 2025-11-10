# DuckDB runtime database configuration (config_db)

A named character list of DuckDB runtime configuration settings used by
the package. This object includes allocator settings, thread counts,
extension flags, storage and checkpoint options, security and secret
settings, and other engine-level options. These settings reflect the
values returned by `md::config_db` and are suitable as a template or
reference for configuring DuckDB instances created via the package.

## Usage

``` r
config_db
```

## Format

A named character list. Example names and values include:

- access_mode:

  character; e.g. "automatic"

- allocator_background_threads:

  character; "true"/"false"

- allocator_bulk_deallocation_flush_threshold:

  character; e.g. "512MB"

- allocator_flush_threshold:

  character; e.g. "128MB"

- allow_community_extensions:

  character; "true"/"false"

- allow_extensions_metadata_mismatch:

  character; "true"/"false"

- allow_persistent_secrets:

  character; "true"/"false"

- allow_unredacted_secrets:

  character; "true"/"false"

- allow_unsigned_extensions:

  character; "true"/"false"

- arrow_large_buffer_size:

  character; "true"/"false"

- arrow_lossless_conversion:

  character; "true"/"false"

- arrow_output_list_view:

  character; "true"/"false"

- autoinstall_extension_repository:

  character; URL or empty string

- autoinstall_known_extensions:

  character; "true"/"false"

- autoload_known_extensions:

  character; "true"/"false"

- ca_cert_file:

  character; path to certificate file, or empty string

- catalog_error_max_schemas:

  character; numeric as string, e.g. "100"

- checkpoint_threshold:

  character; e.g. "16MB"

- wal_autocheckpoint:

  character; e.g. "16MB"

- custom_extension_repository:

  character; URL or empty string

- custom_user_agent:

  character; string or empty

- default_block_size:

  character; e.g. "262144"

- default_collation:

  character; collation name or empty

- default_null_order:

  character; e.g. "NULLS_LAST"

- null_order:

  character; e.g. "NULLS_LAST"

- default_order:

  character; e.g. "ASC"

- default_secret_storage:

  character; e.g. "local_file"

- disabled_compression_methods:

  character; list or empty

- duckdb_api:

  character; e.g. "cli"

- enable_external_access:

  character; "true"/"false"

- enable_external_file_cache:

  character; "true"/"false"

- enable_fsst_vectors:

  character; "true"/"false"

- enable_http_metadata_cache:

  character; "true"/"false"

- enable_macro_dependencies:

  character; "true"/"false"

- enable_object_cache:

  character; "true"/"false"

- enable_server_cert_verification:

  character; "true"/"false"

- enable_view_dependencies:

  character; "true"/"false"

- extension_directory:

  character; path or empty

- external_threads:

  character; numeric as string, e.g. "1"

- force_download:

  character; "true"/"false"

- immediate_transaction_mode:

  character; "true"/"false"

- index_scan_max_count:

  character; numeric as string, e.g. "2048"

- index_scan_percentage:

  character; numeric as string, e.g. "0.001"

- lock_configuration:

  character; "true"/"false"

- max_vacuum_tasks:

  character; numeric as string, e.g. "100"

- old_implicit_casting:

  character; "true"/"false"

- parquet_metadata_cache:

  character; "true"/"false"

- preserve_insertion_order:

  character; "true"/"false"

- produce_arrow_string_view:

  character; "true"/"false"

- scheduler_process_partial:

  character; "true"/"false"

- secret_directory:

  character; path for persistent secrets

- storage_compatibility_version:

  character; e.g. "v0.10.2"

- temp_directory:

  character; path or empty string

- threads:

  character; number of threads as string, e.g. "4"

- worker_threads:

  character; number of threads as string, e.g. "4"

- username:

  character; string or empty

- user:

  character; string or empty

- zstd_min_string_length:

  character; numeric as string, e.g. "4096"

## Examples

``` r
# inspect the config
md::config_db
#> $allocator_background_threads
#> [1] "false"
#> 
#> $allocator_bulk_deallocation_flush_threshold
#> [1] "512MB"
#> 
#> $allocator_flush_threshold
#> [1] "128MB"
#> 
#> $allow_community_extensions
#> [1] "true"
#> 
#> $allow_extensions_metadata_mismatch
#> [1] "false"
#> 
#> $allow_persistent_secrets
#> [1] "true"
#> 
#> $allow_unredacted_secrets
#> [1] "false"
#> 
#> $allow_unsigned_extensions
#> [1] "false"
#> 
#> $arrow_large_buffer_size
#> [1] "false"
#> 
#> $arrow_lossless_conversion
#> [1] "false"
#> 
#> $arrow_output_list_view
#> [1] "false"
#> 
#> $autoinstall_extension_repository
#> [1] ""
#> 
#> $autoinstall_known_extensions
#> [1] "true"
#> 
#> $autoload_known_extensions
#> [1] "true"
#> 
#> $ca_cert_file
#> [1] ""
#> 
#> $catalog_error_max_schemas
#> [1] "100"
#> 
#> $checkpoint_threshold
#> [1] "16MB"
#> 
#> $wal_autocheckpoint
#> [1] "16MB"
#> 
#> $custom_extension_repository
#> [1] ""
#> 
#> $default_block_size
#> [1] "262144"
#> 
#> $default_collation
#> [1] ""
#> 
#> $default_null_order
#> [1] "NULLS_LAST"
#> 
#> $null_order
#> [1] "NULLS_LAST"
#> 
#> $default_order
#> [1] "ASC"
#> 
#> $default_secret_storage
#> [1] "local_file"
#> 
#> $disabled_compression_methods
#> [1] ""
#> 
#> $enable_external_file_cache
#> [1] "true"
#> 
#> $enable_fsst_vectors
#> [1] "false"
#> 
#> $enable_http_metadata_cache
#> [1] "false"
#> 
#> $enable_macro_dependencies
#> [1] "false"
#> 
#> $enable_object_cache
#> [1] "false"
#> 
#> $enable_server_cert_verification
#> [1] "false"
#> 
#> $enable_view_dependencies
#> [1] "false"
#> 
#> $extension_directory
#> [1] ""
#> 
#> $external_threads
#> [1] "1"
#> 
#> $force_download
#> [1] "false"
#> 
#> $immediate_transaction_mode
#> [1] "false"
#> 
#> $index_scan_max_count
#> [1] "2048"
#> 
#> $index_scan_percentage
#> [1] "0.001"
#> 
#> $lock_configuration
#> [1] "false"
#> 
#> $max_vacuum_tasks
#> [1] "100"
#> 
#> $old_implicit_casting
#> [1] "false"
#> 
#> $parquet_metadata_cache
#> [1] "false"
#> 
#> $preserve_insertion_order
#> [1] "true"
#> 
#> $produce_arrow_string_view
#> [1] "false"
#> 
#> $scheduler_process_partial
#> [1] "false"
#> 
#> $secret_directory
#> [1] "~/.duckdb/stored_secrets"
#> 
#> $storage_compatibility_version
#> [1] "v0.10.2"
#> 
#> $temp_directory
#> [1] ""
#> 
#> $threads
#> [1] "4"
#> 
#> $worker_threads
#> [1] "4"
#> 
#> $username
#> [1] ""
#> 
#> $user
#> [1] ""
#> 
#> $zstd_min_string_length
#> [1] "4096"
#> 
# access individual settings
md::config_db$threads
#> [1] "4"
```
