#' @title FeatureHandler
#'
#' @description
#'   This `FeatureHandler` [R6][R6::R6Class] handles individual features for the feature stores.
#'   They define the three methods associated with features (`compute`, `get` and `key_join`).
#' @export
FeatureHandler <- R6::R6Class( # nolint: object_name_linter.
  classname = "FeatureHandler",

  public = list(

    #' @description
    #'   Creates a new instance of the `FeatureHandler` [R6][R6::R6Class] class.
    #' @param compute (`function`)\cr
    #'   A function of the form "function(start_date, end_date, slice_ts, source_conn)".
    #'   This function should compute the feature from the source connection.
    #' @param get (`function`)\cr
    #'   A function of the form "function(target_table, slice_ts, target_conn)".
    #'   This function should retrieve the computed feature from the target connection.
    #' @param key_join (`function`)\cr
    #'   One of the aggregators from [aggregators()].
    #' @return
    #'   A new instance of the `FeatureHandler` [R6][R6::R6Class] class.
    initialize = function(compute = NULL, get = NULL, key_join = NULL) {

      # Determine given args
      args <- as.list(c(compute = compute, get = get, key_join = key_join))

      # Set defaults for missing functions
      if (is.null(get)) {
        args <- append(args, c("get" = function(target_table, slice_ts, target_conn) {
          SCDB::get_table(target_conn, target_table, slice_ts = slice_ts)
        }))
      }
      if (is.null(compute)) {
        args <- append(args, c("compute" = \(...) stop("compute not configured!")))
      }
      if (is.null(key_join)) {
        args  <- append(args, c("key_join" = \(...) stop("key_join not configured!")))
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
