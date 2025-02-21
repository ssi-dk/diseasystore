test_that("nolint_position_linter works", {                                                                             # nolint start: nolint_position_linter
  skip_if_not_installed("lintr")

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
  skip_if_not_installed("lintr")

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


test_that("non_ascii_linter works", {
  skip_if_not_installed("lintr")

  lintr::expect_lint("æ",     list("line_number" = 1, "type" = "style"), non_ascii_linter())                            # nolint start: non_ascii_linter
  lintr::expect_lint("\nø",   list("line_number" = 2, "type" = "style"), non_ascii_linter())
  lintr::expect_lint("\n\nå", list("line_number" = 3, "type" = "style"), non_ascii_linter())
  lintr::expect_lint("æ",     list("line_number" = 1, "type" = "style"), non_ascii_linter())                            # nolint end

  lintr::expect_lint("xxx", NULL, non_ascii_linter())
  lintr::expect_lint("abc", NULL, non_ascii_linter())
})


test_that("param_and_field_linter works", {
  skip_if_not_installed("lintr")

  lintr::expect_lint("#' @param test (type)\\cr",  list("line_number" = 1, "type" = "style"), param_and_field_linter()) # nolint start: param_and_field_linter
  lintr::expect_lint("#' @field test (type)\\cr",  list("line_number" = 1, "type" = "style"), param_and_field_linter())
  lintr::expect_lint("#' @param test (`type`)",    list("line_number" = 1, "type" = "style"), param_and_field_linter())
  lintr::expect_lint("#' @field test (`type`)",    list("line_number" = 1, "type" = "style"), param_and_field_linter()) # nolint end

  lintr::expect_lint("#' @param test (`type`)\\cr", NULL, param_and_field_linter())
  lintr::expect_lint("#' @field test (`type`)\\cr", NULL, param_and_field_linter())
})


test_that("documentation_template_linter works", {
  skip_if_not_installed("lintr")
  skip_if_not_installed("devtools")
  skip_if(!interactive(), "Skip if running in R_check")
  skip_on_cran()

  lintr::expect_lint(
    "#' @param observable text", # rd_observable defined in R/0_documentation.R                                         # nolint: documentation_template_linter, param_and_field_linter
    list("line_number" = 1, "type" = "style"),
    documentation_template_linter()
  )

  lintr::expect_lint(
    "#' @param observable `r rd_test`",
    NULL,
    documentation_template_linter()
  )
})
