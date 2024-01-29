withr::local_options("diseasystore.target_schema" = target_schema_1)

test_that("SCDB gives too many messages", {

  # The following SCDB functions have been giving unwanted messages
  # SCDB::get_tables

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


  DBI::dbDisconnect(conn)
})
