test_that("rd_templates works", {
  pkg_funs <- ls(base::getNamespace("diseasystore"))
  rd_funs <- purrr::keep(pkg_funs, ~ startsWith(., "rd_"))

    for (type in c("field", "param")) {
      for (rd_fun in rd_funs) {
        expect_no_condition(str <- do.call(rd_fun, args = list(type = type)))
        expect_character(str)
      }
    }
})
