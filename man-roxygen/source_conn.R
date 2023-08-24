#' @param source_conn \cr
#'   Used to specify where data is located. <%= ifelse(exists("read_only") && isTRUE(read_only), "Read only.", "")%>
#'   Can be `DBIConnection` or file path depending on the `diseasystore`.
