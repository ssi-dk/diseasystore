#' Existence aware pick operator
#' @param env (`object`)\cr
#'   The object or environment to attempt to pick from
#' @param field (`character`)\cr
#'   The name of the field to pick from `env`
#' @return
#'   Error if the `field` does not exist in `env`, otherwise it returns `field`
#' @examples
#'  t <- list(a = 1, b = 2)
#'
#'  t$a       # 1
#'  t %.% a   # 1
#'
#'  t$c # NULL
#'  try(t %.% c) # Gives error since "c" does not exist in "t"
#' @export
`%.%` <- function(env, field) {
  env_name <- as.character(match.call())[2]
  field <- as.character(match.call())[3]
  field_name <- stringr::str_extract(field, r"{^[a-zA-Z0-9\._]+}")
  field_call <- stringr::str_remove(field, paste0("^", field_name))

  # Ensure env is environment
  if (is.environment(env)) env <- as.list(env, all.names = TRUE)

  # Check for existence
  if (!(field_name %in% names(env))) {
    stop(field_name, " not found in ", env_name)
  }

  # Retrieve the object
  obj <- purrr::pluck(env, field_name)

  # Evoke the operation on the object
  if (field_call == "") {
    return(obj)
  } else {
    return(eval(parse(text = paste0("obj", field_call))))
  }

}
