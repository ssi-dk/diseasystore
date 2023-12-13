#' The custom linters of `diseasy`
#' @name diseasy_linters
#' @description
#' nolint_position_linter: Check that the `nolint:` statements occur after the character limit
#'
#' @param length maximum line length allowed. Default is 80L (Hollerith limit).
#' @returns A list of `lintr::Lint`
#' @examples
#' ## nolint_position_linter
#' # will produce lints
#' lintr::lint(
#'   text = paste0(strrep("x", 15L), "# nolint: object_name_linter"),
#'   linters = c(nolint_position_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' # okay
#' lintr::lint(
#'   text = paste0(strrep("x", 20L), "# nolint: object_name_linter"),
#'   linters = c(nolint_position_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' @seealso
#' - [lintr::linters] for a complete list of linters available in lintr.
#' - <https://style.tidyverse.org/syntax.html#long-lines>
#' @export
#' @importFrom rlang .data
nolint_position_linter <- function(length = 80L) {
  general_msg <- paste("`nolint:` statements start at", length + 1, "characters.")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      nolint_info <- source_expression$content |>
        stringr::str_locate_all(stringr::regex(r"{# *nolint}", ignore_case = TRUE))

      nolint_info <- purrr::map2(
        nolint_info,
        seq_along(nolint_info),
        ~ dplyr::mutate(as.data.frame(.x), line_number = .y)
      ) |>
        purrr::reduce(rbind) |>
        dplyr::filter(!is.na(.data$start)) |>
        dplyr::filter(.data$start <= length)

      purrr::pmap(
        nolint_info,
        \(start, end, line_number) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            column_number = start,
            type = "style",
            message = paste(general_msg, "This statement starts at", start, "characters"),
            line = source_expression$file_lines[line_number],
            ranges = list(c(start, end))
          )
        }
      )
    }
  )
}


#' @name diseasy_linters
#' @description
#' nolint_line_length_linter: Check that lines adhere to a given character limit, ignoring `nolint` statements
#'
#' @param length maximum line length allowed. Default is 80L (Hollerith limit).
#' @examples
#' ## nolint_line_length_linter
#' # will produce lints
#' lintr::lint(
#'   text = paste0(strrep("x", 25L), "# nolint: object_name_linter."),
#'   linters = c(nolint_line_length_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' # okay
#' lintr::lint(
#'   text = paste0(strrep("x", 20L), "# nolint: object_name_linter."),
#'   linters = c(nolint_line_length_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' @export
#' @importFrom rlang .data
nolint_line_length_linter <- function(length = 80L) {
  general_msg <- paste("Lines should not be more than", length, "characters.")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      nolint_regex <- r"{# ?nolint ?(start|end)?:?.*}"

      file_lines_nolint_excluded <- source_expression$file_lines |>
        purrr::map_chr(\(s) stringr::str_remove(s, nolint_regex))

      line_lengths <- nchar(file_lines_nolint_excluded)
      long_lines <- which(line_lengths > length)
      Map(function(long_line, line_length) {
        lintr::Lint(
          filename = source_expression$filename,
          line_number = long_line,
          column_number = length + 1L, type = "style",
          message = paste(general_msg, "This line is", line_length, "characters."),
          line = source_expression$file_lines[long_line],
          ranges = list(c(1L, line_length))
        )
      }, long_lines, line_lengths[long_lines])
    }
  )
}
