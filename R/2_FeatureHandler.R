#' @title FeatureHandler
#'
#' @description
#'   This `FeatureHandler` [R6][R6::R6Class] handles individual features for the feature stores.
#'   They define the three methods associated with features (`compute`, `get` and `key_join`).
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   # The FeatureHandler is typically configured as part of making a new Diseasystore.
#'   # Most often, we need only specify `compute` and `key_join` to get a functioning FeatureHandler
#'
#'   # In this example we use mtcars as the basis for our features
#'   conn <- SCDB::get_connection(drv = RSQLite::SQLite())
#'
#'   # We use mtcars as our basis. First we add the rownames as an actual column
#'   data <- dplyr::mutate(mtcars, key_name = rownames(mtcars), .before = dplyr::everything())
#'
#'   # Then we add some imaginary times where these cars were produced
#'   data <- dplyr::mutate(data,
#'                         production_start = as.Date(Sys.Date()) + floor(runif(nrow(mtcars)) * 100),
#'                         production_end   = production_start + floor(runif(nrow(mtcars)) * 365))
#'
#'   dplyr::copy_to(conn, data, "mtcars")
#'
#'   # In this example, the feature we want is the "maximum miles per gallon"
#'   # The feature in question in the mtcars data set is then "mpg" and when we need to reduce
#'   # our data set, we want to use the "max()" function.
#'
#'   # We first write a compute function for the mpg in our modified mtcars data set
#'   # Our goal is to get the mpg of all cars that were in production at the between start/end_date
#'   compute_mpg <- function(start_date, end_date, slice_ts, source_conn) {
#'     out <- SCDB::get_table(source_conn, "mtcars", slice_ts = slice_ts) |>
#'       dplyr::filter({{ start_date }} <= .data$production_end,
#'                     .data$production_start <= {{ end_date }}) |>
#'       dplyr::transmute("key_name", "mpg",
#'                        "valid_from" = "production_start",
#'                        "valid_until" = "production_end")
#'
#'     return(out)
#'   }
#'
#'   # We can now combine into our FeatureHandler
#'   fh_max_mpg <- FeatureHandler$new(compute = compute_mpg, key_join = key_join_max)
#'
#'   DBI::dbDisconnect(conn)
#' @return
#'   A new instance of the `FeatureHandler` [R6][R6::R6Class] class.
#' @export
#' @importFrom R6 R6Class
FeatureHandler <- R6::R6Class(                                                                                          # nolint: object_name_linter
  classname = "FeatureHandler",

  public = list(

    #' @description
    #'   Creates a new instance of the `FeatureHandler` [R6][R6::R6Class] class.
    #' @param compute (`function`)\cr
    #'   A function of the form "function(start_date, end_date, slice_ts, source_conn)".
    #'   This function should return a `data.frame` with the computed feature (computed from the source connection).
    #'   The `data.frame` should contain the following columns:
    #'    * key_*: One (or more) columns containing keys to link this feature with other features
    #'    * *: One (or more) columns containing the features that are computed
    #'    * valid_from, valid_until: A set of columns containing the time period for which this feature information
    #'    is valid.\cr
    #' @param get (`function`)\cr
    #'   (Optional). A function of the form "function(target_table, slice_ts, target_conn)".
    #'   This function should retrieve the computed feature from the target connection.\cr
    #' @param key_join (`function`)\cr
    #'   A function like one of the aggregators from [aggregators()].
    #'
    #'   The function should return an expression on the form:
    #'   dplyr::summarise(.data,
    #'     dplyr::across(.cols = tidyselect::all_of(feature),
    #'                   .fns = list(n = ~ aggregation function),
    #'                   .names = "\{.fn\}"),
    #'     .groups = "drop")
    #' @return
    #'   A new instance of the `FeatureHandler` [R6][R6::R6Class] class.
    initialize = function(compute = NULL, get = NULL, key_join = NULL) {

      # Determine given args
      args <- as.list(c(compute = compute, get = get, key_join = key_join))

      # Set defaults for missing functions
      if (is.null(compute)) {
        args <- append(args, c("compute" = \(...) stop("compute not configured!")))
      } else {
        checkmate::assert_function(compute, args = c("start_date", "end_date", "slice_ts", "source_conn"))
      }

      if (is.null(get)) {
        args <- append(args, c("get" = function(target_table, slice_ts, target_conn) {
          SCDB::get_table(target_conn, target_table, slice_ts = slice_ts)
        }))
      } else {
        checkmate::assert_function(get, args = c("target_table", "slice_ts", "target_conn"))
      }

      if (is.null(key_join)) {
        args  <- append(args, c("key_join" = \(...) stop("key_join not configured!")))
      } else {
        checkmate::assert_function(key_join, args = c(".data", "feature"))
      }

      # Set the functions of the FeatureHandler
      purrr::walk2(args, names(args), ~ {
        dot_name <- paste0(".", .y)
        purrr::pluck(private, dot_name) <- .x
      })
    }
  ),

  active = list(

    #' @field compute (`function`)\cr
    #'   A function of the form "function(start_date, end_date, slice_ts, source_conn)".
    #'   This function should compute the feature from the source connection.
    compute = function() private$.compute,

    #' @field get (`function`)\cr
    #'   A function of the form "function(target_table, slice_ts, target_conn)".
    #'   This function should retrieve the computed feature from the target connection.
    get = function() private$.get,

    #' @field key_join (`function`)\cr
    #'   One of the aggregators from [aggregators].
    key_join = function() private$.key_join
  ),

  private = list(
    .get = NULL,
    .compute  = NULL,
    .key_join = NULL
  )
)
