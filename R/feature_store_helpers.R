#' Transform case definition to PascalCase
#' @template case_definition
diseasystore_case_definition <- function(case_definition) {

  case_definition |>
    stringr::str_to_title() |>
    stringr::str_replace_all(" ", "") |>
    stringr::str_replace_all("-", "") |>
    (\(.)paste0("Diseasystore", .))()

}


#' Detect available diseasystores
#' @export
available_diseasy_stores <- function() {
  ls("package:diseasystore") |>
    purrr::keep(~ startsWith(., "Diseasystore")) |>
    purrr::discard(~ . %in% c("DiseasystoreBase", "DiseasystoreGeneric"))
}

#' Check for the existence of a `diseasystore` for the case definition
#' @template case_definition
#' @export
feature_store_exists <- function(case_definition) {

  checkmate::assert_character(case_definition)

  # Convert case_definition to DiseasystorePascalCase and check existence
  return(diseasystore_case_definition(case_definition) %in% available_diseasy_stores())

}

#' Get the `diseasystore` for the case definition
#' @template case_definition
#' @export
get_feature_store <- function(case_definition) {

  checkmate::assert_true(feature_store_exists(case_definition))

  return(get(diseasystore_case_definition(case_definition)))
}

# #' @importFrom rlang .data
# #' @export
# drop_feature_store <- function(conn, pattern = NULL) {

#   # Get tables to delete
#   tables_to_delete <- mg_get_tables(conn, pattern) |>
#     dplyr::filter(.data$schema == "fs")

#   # Check if logs is in the table, if yes, all tables must be deleted
#   if ("logs" %in% tables_to_delete$table &&
#       !identical(tables_to_delete, dplyr::filter(mg_get_tables(conn), .data$schema == private %.% target_schema))) {
#     stop("'fs.logs' set to delete. Can only delete if entire featurestore is dropped.")
#   }

#   tables_to_delete |>
#     purrr::pwalk(~ DBI::dbExecute(conn, glue::glue('DROP TABLE "{..1}"."{..2}"')))

#   # Delete from logs
#   if (mg_table_exists(conn, "fs.logs")) {
#     purrr::walk(tables_to_delete$table,
#      ~ DBI::dbExecute(conn, glue::glue("DELETE FROM fs.logs WHERE logs.table = '{.x}'")))
#   }
# }

interlace <- function(primary, secondary = NULL) {

  # Check edge case
  if (is.null(secondary)) return(primary)

  # Determine the keys of the primary data
  primary_keys <- colnames(primary)[startsWith(colnames(primary), "key_")]

  # Find all secondary information that is vaild while primary information is valid
  secondary_truncated <- secondary |>
    purrr::map(
      ~ {
          common_keys <- intersect(primary_keys, colnames(.x)[startsWith(colnames(.x), "key_")])
          dplyr::right_join(x = .x, y = primary, suffix = c(".x", ""), by = common_keys) |>
            dplyr::filter((valid_from  < valid_until.x) | is.null(valid_from.x), # Match within validity OR no match
                          (valid_until > valid_from.x)  | is.null(valid_until) | is.null(valid_until.x)) |>
            dplyr::mutate(valid_from  = pmax(valid_from,  valid_from.x,  na.rm = TRUE),
                          valid_until = pmin(valid_until, valid_until.x, na.rm = TRUE)) |>
            dplyr::select(-tidyselect::ends_with(".x"))
      })

  # With the secondary data truncated, we can interlace and return
  out <- mg_interlace_sql(secondary_truncated, by = purrr::pluck(primary_keys, 1))

  return(out)
}

# Existence operator
`%.%` <- function(env, field) {
  field_name <- as.character(substitute(field))
  env_name <- as.character(substitute(env))
  if (is.environment(env)) env <- as.list(env)
  if (!(field_name %in% names(env))) {
    stop(field_name, " not found in ", env_name)
  }
  return(purrr::pluck(env, field_name))
}
