test_that("truncate_interlace works", {

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  # Lets create some synthetic data
  data <- dplyr::mutate(mtcars, "key_name" = rownames(mtcars))

  x <- dplyr::select(data, key_name, mpg, cyl)
  y <- dplyr::select(data, key_name, wt, vs)
  z <- dplyr::select(data, key_name, qsec)

  # In x, the mpg was changed on 2000-01-01 for all but the first 10 cars
  x <- list(utils::head(x, 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date(NA)),
            utils::tail(x, base::nrow(x) - 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date("2000-01-01")),
            utils::tail(x, base::nrow(x) - 10) |>
              dplyr::mutate("mpg" = 0.9 * mpg,
                            "valid_from" = as.Date("2000-01-01"), "valid_until" = as.Date(NA))) |>
    purrr::reduce(union_all) %>%
    dplyr::copy_to(conn, ., name = "x", overwrite = TRUE)

  # In y, the wt was changed on 2010-01-01 for all but the last 10 cars
  y <- list(utils::head(y, base::nrow(y) - 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date(NA)),
            utils::tail(y, 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date("2010-01-01")),
            utils::tail(y, 10) |>
              dplyr::mutate(wt = 1.1 * wt,
                            "valid_from" = as.Date("2010-01-01"), "valid_until" = as.Date(NA))) |>
    purrr::reduce(union_all) %>%
    dplyr::copy_to(conn, ., name = "y", overwrite = TRUE)


  # In z, the qsec was changed on 2020-01-01 for all but the last and first 10 cars
  z <- list(utils::head(z, base::nrow(z) - 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date(NA)),
            utils::tail(z, 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date(NA)),
            utils::head(z, base::nrow(z) - 10) |>
              utils::tail(base::nrow(z) - 10) |>
              dplyr::mutate("valid_from" = as.Date("1990-01-01"), "valid_until" = as.Date("2020-01-01")),
            utils::head(z, base::nrow(z) - 10) |>
              utils::tail(base::nrow(z) - 10) |>
              dplyr::mutate(qsec = 1.1 * qsec,
                            "valid_from" = as.Date("2020-01-01"), "valid_until" = as.Date(NA))) |>
    purrr::reduce(union_all) %>%
    dplyr::copy_to(conn, ., name = "z", overwrite = TRUE)


  # Copy data to db
  data <- dplyr::copy_to(conn, data, overwrite = TRUE)


  # We choose a couple of primary interval to test with
  p1 <- dplyr::transmute(data, key_name, valid_from = as.Date("1985-01-01"), valid_until = as.Date(NA))
  p2 <- dplyr::transmute(data, key_name, valid_from = as.Date("1995-01-01"), valid_until = as.Date("2005-01-01"))
  p3 <- dplyr::transmute(data, key_name, valid_from = as.Date("2005-01-01"), valid_until = as.Date("2015-01-01"))


  # We define a small helper function to test the interlace outputs
  interlace_tester <- function(primary, secondary, output) {

    primary <- dplyr::collect(primary)
    secondary <- purrr::map(secondary, dplyr::collect)
    output  <- dplyr::collect(output)

    # The validity of the output should truncated by the validity of the primary table
    min_date_secondary <- purrr::reduce(purrr::map(secondary, ~ min(.x$valid_from)), min)
    min_date <- max(min_date_secondary, min(primary$valid_from))

    max_date_secondary <- purrr::reduce(purrr::map(secondary, ~ max(.x$valid_until)), max)
    max_date <- min(max_date_secondary, max(primary$valid_until))

    expect_equal(min(output$valid_from),  min_date)
    expect_equal(max(output$valid_until), max_date)

  }

  # No change when no secondary is given
  expect_equal(truncate_interlace(p1), p1)
  expect_equal(truncate_interlace(p1, NULL), p1)


  # Check that a few permutations work as expected
  interlace_tester(p1, list(x),
                   truncate_interlace(p1, list(x)))

  interlace_tester(p1, list(x, y),
                   truncate_interlace(p1, list(x, y)))

  interlace_tester(p1, list(x, y, z),
                   truncate_interlace(p1, list(x, y, z)))



  interlace_tester(p2, list(x),
                   truncate_interlace(p2, list(x)))

  interlace_tester(p2, list(x, y),
                   truncate_interlace(p2, list(x, y)))

  interlace_tester(p2, list(x, y, z),
                   truncate_interlace(p2, list(x, y, z)))



  interlace_tester(p3, list(x),
                   truncate_interlace(p3, list(x)))

  interlace_tester(p3, list(x, y),
                   truncate_interlace(p3, list(x, y)))

  interlace_tester(p3, list(x, y, z),
                   truncate_interlace(p3, list(x, y, z)))



  # Check other permutations
  expect_mapequal(truncate_interlace(p3, list(x, y, z)) |> dplyr::collect(),
                  truncate_interlace(p3, list(y, x, z)) |> dplyr::collect())


  # Check that list conversion works
  expect_identical(truncate_interlace(x, y),
                   truncate_interlace(x, list(y)))

  # Clean up
  DBI::dbDisconnect(conn)

})
