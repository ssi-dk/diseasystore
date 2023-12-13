test_that("nolint_position_linter works", {                                                                             # nolint start: nolint_position_linter
  lintr::expect_lint(
    paste0(strrep("x", 5), "# nolint: object_name_linter."),
    list("column_number" = 6, "type" = "style"),
    c(nolint_position_linter(length = 20L), lintr::object_name_linter())
  )

  lintr::expect_lint(
    paste0(strrep("x", 20), "# nolint: object_name_linter."),
    NULL,
    c(nolint_position_linter(length = 20L), lintr::object_name_linter())
  )
})                                                                                                                      # nolint end


test_that("nolint_line_length_linter works", {                                                                          # nolint start: nolint_position_linter
  lintr::expect_lint(
    paste0(strrep("x", 5), "# nolint: object_name_linter."),
    list("column_number" = 5, "type" = "style"),
    c(nolint_line_length_linter(length = 4L), lintr::object_name_linter())
  )

  lintr::expect_lint(
    paste0(strrep("x", 5), "# nolint: object_name_linter."),
    NULL,
    c(nolint_line_length_linter(length = 5L), lintr::object_name_linter())
  )
})                                                                                                                      # nolint end
