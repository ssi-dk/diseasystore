test_that("SCDB gives too many messages", {
  testthat::skip_on_cran()

  # The following SCDB functions have been giving unwanted messages
  # SCDB::get_tables
  # SCDB::is_historical

  # As well as calls to dplyr::tbl if a SCDB::id is supplied

  # Here we test whether these still give messages



  # SCDB::get_tables -- privileges warning (tempfile)
  conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = tempfile())

  message_as_expected <- tryCatch(
    SCDB::get_tables(conn),
    warning = function(w) {
      if (checkmate::test_character(w$message, pattern = "Check user privileges / database configuration")) {
        return(TRUE) # As expected
      } else {
        warning(w) # No warning, we need to remove suppress*
      }
    }
  )
  if (!isTRUE(message_as_expected)) warning("warning no longer being thrown -- superfluous table creation in setup.R")

  if (packageVersion("SCDB") < "0.4.0") {
    # Try to write to the target schema
    test_id <- SCDB::id("test.mtcars", conn)

    # Check write permissions
    DBI::dbWriteTable(conn, test_id, mtcars)

    # SCDB::is_historical
    message_as_expected <- tryCatch(
      SCDB::is.historical(dplyr::tbl(conn, test_id)),
      message = function(m) {
        if (checkmate::test_character(m$message, pattern = "It looks like you tried to incorrectly use")) {
          return(TRUE) # As expected
        } else {
          warning(m) # No message, we need to remove suppress*
        }
      }
    )
    if (!isTRUE(message_as_expected)) warning("warning no longer being thrown -- remove suppressWarnings")
  }


  DBI::dbDisconnect(conn)
})
