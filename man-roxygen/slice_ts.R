#' @param slice_ts (`Date` or `character`)\cr
#'   Date to slice the database on (used if source_conn is a database). <%= ifelse(exists("read_only") && isTRUE(read_only), "Read only.", "")%>
