test_that("age_labels() works", {

  # Test simple use case
  expect_identical(
    age_labels(c(5, 12, 20, 30)),
    c("00-04", "05-11", "12-19", "20-29", "30+")
  )

  # Test superfluous zeros are ignored
  expect_identical(
    age_labels(c(0, 5, 12, 20, 30)),
    c("00-04", "05-11", "12-19", "20-29", "30+")
  )

  # Test zero-padding works for large ages
  expect_identical(
    age_labels(c(5, 12, 20, 30, 130)),
    c("000-004", "005-011", "012-019", "020-029", "030-129", "130+")
  )
})


# Create a test data set that contains all birth dates in 2000, 1970, and 1940 (years with, or almost with, leap years)
test_data <- c(
  seq.Date(from = as.Date("2000-01-01"), to = as.Date("2000-12-31"), by = "1 day"),
  seq.Date(from = as.Date("1970-01-01"), to = as.Date("1970-12-31"), by = "1 day"),
  seq.Date(from = as.Date("1940-01-01"), to = as.Date("1940-12-31"), by = "1 day")
) |>
  tibble::tibble(birth_date = _) |>
  dplyr::mutate(reference_date = birth_date + lubridate::days(40 * dplyr::row_number()))


test_that("age_on_date() works for date input", {

  test_date <- as.Date("2024-02-28")

  for (conn in get_test_conns()) {

    # Copy to the remote
    test_ages <- dplyr::copy_to(conn, test_data, "test_age")
    SCDB::defer_db_cleanup(test_ages)

    # Compute the reference age using lubridate
    reference_ages <- test_data |>
      dplyr::mutate(age = lubridate::year(lubridate::as.period(lubridate::interval(.data$birth_date, test_date))))


    # SQLite does not have a precise way to estimate age, so the age computation helper will throw a warning
    # We here test that the warning is thrown, and filter out the known offending dates from the test
    if (inherits(conn, "SQLiteConnection")) {

      # Capture and check warning
      test_ages <- tryCatch(
        dplyr::mutate(test_ages, "age" = !!age_on_date("birth_date", test_date, conn)),
        warning = function(w) {
          expect_match(w$message, "Age computation on SQLite is not precise")
          return(suppressWarnings(dplyr::mutate(test_ages, "age" = !!age_on_date("birth_date", test_date, conn))))
        }
      )

      # Remove offending dates
      test_ages      <- dplyr::filter(test_ages,      .data$birth_date != !!as.numeric(as.Date("1970-02-28")))
      reference_ages <- dplyr::filter(reference_ages, .data$birth_date != as.Date("1970-02-28"))

    } else {

      # Compute the age using the age function
      test_ages <- dplyr::mutate(test_ages, "age" = !!age_on_date("birth_date", test_date, conn))

    }

    expect_identical(
      dplyr::pull(test_ages,      "age"),
      dplyr::pull(reference_ages, "age")
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("age_on_date() works for reference input", {

  for (conn in get_test_conns()) {

    # Copy to the remote
    test_ages <- dplyr::copy_to(conn, test_data, "test_age")
    SCDB::defer_db_cleanup(test_ages)

    # Compute the reference age using lubridate
    reference_ages <- test_data |>
      dplyr::mutate(age = floor(lubridate::interval(.data$birth_date, .data$reference_date) / lubridate::years(1)))


    # SQLite does not have a precise way to estimate age, so the age computation helper will throw a warning
    # We here test that the warning is thrown, and filter out the known offending dates from the test
    if (inherits(conn, "SQLiteConnection")) {

      # Capture and check warning
      test_ages <- tryCatch(
        dplyr::mutate(test_ages, "age" = !!age_on_date("birth_date", "reference_date", conn)),
        warning = function(w) {
          expect_match(w$message, "Age computation on SQLite is not precise")
          return(
            suppressWarnings(dplyr::mutate(test_ages, "age" = !!age_on_date("birth_date", "reference_date", conn)))
          )
        }
      )

      # Remove offending dates
      test_ages      <- dplyr::filter(test_ages,      .data$birth_date != !!as.numeric(as.Date("2000-07-28")))
      reference_ages <- dplyr::filter(reference_ages, .data$birth_date != as.Date("2000-07-28"))

    } else {

      # Compute the age using the age function
      test_ages <- dplyr::mutate(test_ages, "age" = !!age_on_date("birth_date", "reference_date", conn))

    }

    expect_identical(
      dplyr::pull(test_ages,      "age"),
      dplyr::pull(reference_ages, "age")
    )

    DBI::dbDisconnect(conn)
  }
})





# Create a test data set that contains all birth dates in 2000, 1970, and 1940 (years with, or almost with, leap years)
test_data <- c(
  seq.Date(from = as.Date("2000-01-01"), to = as.Date("2000-12-31"), by = "1 day"),
  seq.Date(from = as.Date("1970-01-01"), to = as.Date("1970-12-31"), by = "1 day"),
  seq.Date(from = as.Date("1940-01-01"), to = as.Date("1940-12-31"), by = "1 day")
) |>
  tibble::tibble(birth_date = _) |>
  dplyr::mutate(years_to_add = 1)


test_that("add_years() works for positive input", {

  for (conn in get_test_conns()) {

    # Copy to the remote
    test_ages <- dplyr::copy_to(conn, test_data, "test_age")
    SCDB::defer_db_cleanup(test_ages)

    # Compute the reference date using lubridate
    reference_ages <- test_data |>
      dplyr::mutate("first_birthday" = lubridate::`%m+%`(.data$birth_date, lubridate::years(1)))


    # SQLite does not have a precise way to estimate time, so the time computation helper will throw a warning
    # We here test that the warning is thrown, and filter out the known offending dates from the test
    if (inherits(conn, "SQLiteConnection")) {

      # Capture and check warning
      test_ages <- tryCatch(
        dplyr::mutate(test_ages, "first_birthday" = !!add_years("birth_date", 1, conn)),
        warning = function(w) {
          expect_match(w$message, "Time computation on SQLite is not precise")
          return(suppressWarnings(dplyr::mutate(test_ages, "first_birthday" = !!add_years("birth_date", 1, conn))))
        }
      )


      # Almost all dates in leap-years fail on SQLite
      good_dates <- test_data |>
        dplyr::filter(
          !lubridate::leap_year(.data$birth_date) |
            (lubridate::leap_year(.data$birth_date) & lubridate::month(.data$birth_date) >= 3)
        ) |>
        dplyr::pull("birth_date")


      test_ages <- test_ages |>
        dplyr::filter(.data$birth_date %in% !!as.numeric(good_dates))

      reference_ages <- reference_ages |>
        dplyr::filter(.data$birth_date %in% good_dates) |>
        dplyr::mutate("first_birthday" = as.numeric(.data$first_birthday))

    } else {

      # Compute the date using the add_years function
      test_ages <- dplyr::mutate(test_ages, "first_birthday" = !!add_years("birth_date", 1, conn))

    }

    expect_identical(
      dplyr::pull(test_ages,      "first_birthday"),
      dplyr::pull(reference_ages, "first_birthday")
    )

    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
})


test_that("add_years() works for negative input", {

  for (conn in get_test_conns()) {

    # Copy to the remote
    test_ages <- dplyr::copy_to(conn, test_data, "test_age")
    SCDB::defer_db_cleanup(test_ages)

    # Compute the reference date using lubridate
    reference_ages <- test_data |>
      dplyr::mutate("test_date" = lubridate::`%m-%`(.data$birth_date, lubridate::years(1)))


    # SQLite does not have a precise way to estimate time, so the time computation helper will throw a warning
    # We here test that the warning is thrown, and filter out the known offending dates from the test
    if (inherits(conn, "SQLiteConnection")) {

      # Capture and check warning
      test_ages <- tryCatch(
        dplyr::mutate(test_ages, "test_date" = !!add_years("birth_date", -1, conn)),
        warning = function(w) {
          expect_match(w$message, "Time computation on SQLite is not precise")
          return(suppressWarnings(dplyr::mutate(test_ages, "test_date" = !!add_years("birth_date", -1, conn))))
        }
      )

      # Almost all dates in leap-years fail on SQLite
      good_dates <- test_data |>
        dplyr::filter(
          !lubridate::leap_year(.data$birth_date) |
            (lubridate::leap_year(.data$birth_date) &
               (lubridate::month(.data$birth_date) == 1 |
                  (lubridate::month(.data$birth_date) == 2 & lubridate::day(.data$birth_date) < 29)))
        ) |>
        dplyr::pull("birth_date")


      test_ages <- test_ages |>
        dplyr::filter(.data$birth_date %in% !!as.numeric(good_dates))

      reference_ages <- reference_ages |>
        dplyr::filter(.data$birth_date %in% good_dates) |>
        dplyr::mutate("test_date" = as.numeric(.data$test_date))

    } else {

      # Compute the date using the add_years function
      test_ages <- dplyr::mutate(test_ages, "test_date" = !!add_years("birth_date", -1, conn))

    }

    expect_identical(
      dplyr::pull(test_ages,      "test_date"),
      dplyr::pull(reference_ages, "test_date")
    )

    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
})


test_that("add_years() works for reference input", {

  for (conn in get_test_conns()) {

    # Copy to the remote
    test_ages <- dplyr::copy_to(conn, test_data, "test_age")
    SCDB::defer_db_cleanup(test_ages)

    # Compute the reference date using lubridate
    reference_ages <- test_data |>
      dplyr::mutate("first_birthday" = lubridate::`%m+%`(.data$birth_date, lubridate::years(1)))

    # SQLite does not have a precise way to estimate time, so the time computation helper will throw a warning
    # We here test that the warning is thrown, and filter out the known offending dates from the test
    if (inherits(conn, "SQLiteConnection")) {

      # Capture and check warning
      test_ages <- tryCatch(
        dplyr::mutate(test_ages, "first_birthday" = !!add_years("birth_date", "years_to_add", conn)),
        warning = function(w) {
          expect_match(w$message, "Time computation on SQLite is not precise")
          return(
            suppressWarnings(
              dplyr::mutate(test_ages, "first_birthday" = !!add_years("birth_date", "years_to_add", conn))
            )
          )
        }
      )

      # Almost all dates in leap-years fail on SQLite
      good_dates <- test_data |>
        dplyr::filter(
          !lubridate::leap_year(.data$birth_date) |
            (lubridate::leap_year(.data$birth_date) & lubridate::month(.data$birth_date) >= 3)
        ) |>
        dplyr::pull("birth_date")


      test_ages <- test_ages |>
        dplyr::filter(.data$birth_date %in% !!as.numeric(good_dates))

      reference_ages <- reference_ages |>
        dplyr::filter(.data$birth_date %in% good_dates) |>
        dplyr::mutate("first_birthday" = as.numeric(.data$first_birthday))

    } else {

      # Compute the date using the add_years function
      test_ages <- dplyr::mutate(test_ages, "first_birthday" = !!add_years("birth_date", "years_to_add", conn))

    }

    expect_identical(
      dplyr::pull(test_ages,      "first_birthday"),
      dplyr::pull(reference_ages, "first_birthday")
    )

    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
})


# Create a test data set that contains different number of years to add to a fixed reference date
test_data <- tibble::tibble(years_to_add = seq_len(50) - 1)

test_that("add_years() works for date input", {

  for (conn in get_test_conns()) {

    # Copy to the remote
    test_ages <- dplyr::copy_to(conn, test_data, "test_age")
    SCDB::defer_db_cleanup(test_ages)


    # Set the reference date for test
    reference_date <- as.Date("1970-01-01")

    # Compute the reference date using lubridate
    reference_ages <- test_data |>
      dplyr::mutate("future_date" = lubridate::`%m+%`(reference_date, lubridate::years(years_to_add)))

    # SQLite does not have a precise way to estimate time, so the time computation helper will throw a warning
    # We here test that the warning is thrown, and filter out the known offending dates from the test
    if (inherits(conn, "SQLiteConnection")) {

      # Capture and check warning
      test_ages <- tryCatch(
        dplyr::mutate(test_ages, "future_date" = !!add_years(reference_date, "years_to_add", conn)),
        warning = function(w) {
          expect_match(w$message, "Time computation on SQLite is not precise")
          return(
            suppressWarnings(
              dplyr::mutate(test_ages, "future_date" = !!add_years(reference_date, "years_to_add", conn))
            )
          )
        }
      )

      # 2000 is not a leap year, so for SQLite, everything after this date will fail
      max_interval_to_add <- reference_ages |>
        dplyr::filter(future_date < "2000-01-01") |>
        dplyr::pull("years_to_add") |>
        max()


      test_ages <- test_ages |>
        dplyr::filter(.data$years_to_add <= max_interval_to_add)

      reference_ages <- reference_ages |>
        dplyr::filter(.data$years_to_add <= max_interval_to_add) |>
        dplyr::mutate("future_date" = as.numeric(.data$future_date))

    } else {

      # Compute the date using the add_years function
      test_ages <- dplyr::mutate(test_ages, "future_date" = !!add_years(reference_date, "years_to_add", conn))

    }

    expect_identical(
      dplyr::pull(test_ages,      "future_date"),
      dplyr::pull(reference_ages, "future_date")
    )

    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
})
