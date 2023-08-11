#' Helper function to generate active bindings that are read_only
#' @param value The value attempted to be set
#' @param expr  The expression to execute when called
#' @param name  The name of the active binding
active_binding <- function(value, expr, name) {
  if (missing(value)) {
    eval.parent(expr, n = 1)
  } else {
    read_only_error(name)
  }
}


#' Helper function to produce a "read only" error
#' @param field The name of the field that is read only
read_only_error <- function(field) {
  stop(glue::glue("`${field}` is read only"), call. = FALSE)
}


#' cat printing with default new line
#' @param ...  The normal input to cat
#' @param file Path of an output file to append the output to
#' @param sep The separator given to cat
printr <- function(..., file = "/dev/null", sep = "") {
  sink(file = file, split = TRUE, append = TRUE, type = "output")
  cat(..., "\n", sep = sep)
  sink()
}


#' Helper function to get option
#' @param option (`character`)\cr
#'   Name of the option to get
#' @param self (`R6::R6class Diseasy* instance`)\cr
#'   The object the option applies to.
diseasyoption <- function(option, self) {
  base_class <- stringr::str_extract(class(self)[1], r"{^([A-Z][a-z]*)}") |>
    stringr::str_to_lower()

  list(class(self)[1], NULL) |>
    purrr::map(~ paste(c(base_class, .x, option), collapse = ".")) |>
    purrr::map(getOption) |>
    purrr::keep(purrr::negate(is.null)) |>
    purrr::discard(~ is.character(.) && . == "") |>
    purrr::pluck(1)
}
