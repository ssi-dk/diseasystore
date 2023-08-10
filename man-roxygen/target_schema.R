#' @param target_schema (`character`)\cr
#'   The schema to place the feature store in. <%= ifelse(exists("read_only") && isTRUE(read_only), "Read only.", "")%>
#'   If the database backend does not support schema, the tables will be prefixed with target_schema.
