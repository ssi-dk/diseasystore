#' @param target_conn (`DBIConnection`)\cr
#'   A database connection to store the computed features in. <%= ifelse(exists("read_only") && isTRUE(read_only), "Read only.", "")%>
