#' simulist_data
#'
#' This data set contains a synthetic line list created with the `simulist` package.
#' This line list is used as an example data set for the `DiseasystoreSimulist` class.
#'
#' The data set consists of a `tibble` with columns:
#' - `id`: A unique identifier for each individual.
#' - `case_type`: The type of case. One of "suspected", "probable", "confirmed".
#' - `sex`: The sex of the individual.
#' - `birth`: The birth date of the individual.
#' - `age`: The age of the individual.
#' - `date_onset`: The date of onset of symptoms.
#' - `date_admission`: The date of admission to hospital.
#' - `date_discharge`: The date of discharge from hospital.
#' - `date_death`: The date of death.
#'
#' @name simulist_data
#' @docType data
#' @author Rasmus Skytte Randl\\u00F8v \email{rske@ssi.dk}
#' @source
#'   Lambert J, Tamayo C (2024).
#'   *simulist: Simulate Disease Outbreak Line List and Contacts Data*.
#'   \doi{10.5281/zenodo.10471458}, https://epiverse-trace.github.io/simulist/.
#' @keywords data internal
NULL
