#' nolint linter
#'
#' Check that the `nolint:` statements occur after the character limit
#'
#' @param length maximum line length allowed. Default is 80L (Hollerith limit).
#'
#' @examples
#' # will produce lints
#' lint(
#'   text = paste(strrep("x", 15L), "# nolint: nolint_linter",
#'   linters = nolint_linter(length = 20L)
#' )
#'
#' # okay
#' lint(
#'   text = paste(strrep("x", 19L), "# nolint: nolint_linter",
#'   linters = nolint_linter(length = 20L)
#' )
#'
#' @seealso
#' - [linters] for a complete list of linters available in lintr.
#' - <https://style.tidyverse.org/syntax.html#long-lines>
#' @export
nolint_linter <- function(length = 80L) {
  general_msg <- paste("`nolint:` statements start at", length, "characters.")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      nolint_info <- tibble::tibble(lines = source_expression$file_lines) |>
        dplyr::mutate(line_number = dplyr::row_number()) |>
        dplyr::filter(stringr::str_detect(.data$lines, r"{# ?nolint:}")) |>
        dplyr::group_by(.data$line_number) |>
        dplyr::group_modify(~ {
          stringr::str_locate_all(.$lines, r"{# ?nolint:}") |>
            purrr::map(as.data.frame) |>
            purrr::reduce(union)
        }) |>
        dplyr::filter(start != length + 1)

      purrr::pmap(
        nolint_info,
        \(start, end, line_number) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            column_number = start,
            type = "style",
            message = paste(general_msg, "This `nolint:` statements starts at ", start, "characters."),
            line = source_expression$file_lines[line_number],
            ranges = list(c(start, end))
          )
        }
      )
    }
  )
}
