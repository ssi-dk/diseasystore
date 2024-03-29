% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/drop_diseasystore.R
\name{drop_diseasystore}
\alias{drop_diseasystore}
\title{Drop feature stores from DB}
\usage{
drop_diseasystore(
  pattern = NULL,
  schema = diseasyoption("target_schema"),
  conn = SCDB::get_connection()
)
}
\arguments{
\item{pattern}{(\code{character(1)})\cr
Pattern to match the tables by}

\item{schema}{(\code{character(1)})\cr
Schema the diseasystore uses to store data in}

\item{conn}{(\code{DBIConnection})\cr A database connection}
}
\value{
NULL (called for side effects)
}
\description{
Drop feature stores from DB
}
\examples{
\dontshow{if (requireNamespace("RSQLite", quietly = TRUE)) (if (getRversion() >= "3.4") withAutoprint else force)(\{ # examplesIf}
  conn <- SCDB::get_connection(drv = RSQLite::SQLite())

  drop_diseasystore(conn = conn)

  DBI::dbDisconnect(conn)
\dontshow{\}) # examplesIf}
}
