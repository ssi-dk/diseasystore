test_that("key_join_sum works", {
  expect_equal(mtcars |> key_join_sum("cyl") |> dplyr::pull("n"),
               mtcars |> dplyr::pull("cyl") |> sum())

  expect_equal(mtcars |> key_join_sum("vs") |> dplyr::pull("n"),
               mtcars |> dplyr::pull("vs") |> sum())
})


test_that("key_join_max works", {
  expect_equal(mtcars |> key_join_max("cyl") |> dplyr::pull("n"),
               mtcars |> dplyr::pull("cyl") |> max())

  expect_equal(mtcars |> key_join_max("vs") |> dplyr::pull("n"),
               mtcars |> dplyr::pull("vs") |> max())
})


test_that("key_join_min works", {
  expect_equal(mtcars |> key_join_min("cyl") |> dplyr::pull("n"),
               mtcars |> dplyr::pull("cyl") |> min())

  expect_equal(mtcars |> key_join_min("vs") |> dplyr::pull("n"),
               mtcars |> dplyr::pull("vs") |> min())
})


test_that("key_join_count works", {
  expect_equal(mtcars |> dplyr::rename("key_cyl" = "cyl") |> key_join_count() |> dplyr::pull("n"),
               nrow(mtcars))
})
