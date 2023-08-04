#' @param value The value attempted to be set
#' @param expr  The expression to execute when called
#' @param name  The name of the active binding
#' @export
active_binding <- function(value, expr, name) {
  if (missing(value)) {
    eval.parent(expr, n = 1)
  } else {
    read_only_error(name)
  }
}


#' @param field The name of the field that is read only
#' @export
read_only_error <- function(field) {
  stop(glue::glue("`${field}` is read only"), call. = FALSE)
}
