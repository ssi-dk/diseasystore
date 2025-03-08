% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/drop_diseasystore.R
\name{drop_diseasystore}
\alias{drop_diseasystore}
\title{Drop feature stores from DB}
\usage{
drop_diseasystore(
  pattern = NULL,
  schema = diseasyoption("target_schema", namespace = "diseasystore"),
  conn = SCDB::get_connection()
)
}
\arguments{
\item{pattern}{(\code{character(1)})\cr
Pattern to match the tables by}

\item{schema}{(\code{character})\cr A database schema.  If the database backend does not support schema, the tables will be prefixed with \verb{<schema>.}
The location where the \code{diseasystore} stores data.}

\item{conn}{(\code{DBIConnection} or \code{function})\cr A database connection or function that opens a database connection.}
}
\value{
\code{NULL} (called for side effects)
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
