package org.dcs.api.error

/**
  * Factory for all known error responses, divided into
  * relevant categories in blocks of 100 each.
  *
  * Note: The first error for each category (i.e. 100, 200, etc)
  * is reserved and should be used to representing 'any' error
  * for the corresponding category.
  *
  * Created by cmathew on 05/06/16.
  */
object ErrorConstants {
  val UnknownErrorResponse = ErrorResponse("DCS001", "Unknown error", 500)

  /*
    General errors
   */
  val DCS001 = UnknownErrorResponse
  val DCS002 =  ErrorResponse("DCS002", "Unexpected Error", 500)

  /*
    Unauthorised request
   */
  val DCS003 =  ErrorResponse("DCS003", "Request is not authorised", 401)

  /*
    Data related errors
   */
  val DCS101 = ErrorResponse("DCS101", "Datasource with given name already exists", 406)
  val DCS102 = ErrorResponse("DCS102", "Error loading data", 500)
  val DCS103 = ErrorResponse("DCS103", "Error initializing data store", 500)
  val DCS104 = ErrorResponse("DCS104", "Error reading data", 500)
  val DCS105 = ErrorResponse("DCS105", "Error writing data", 500)
  val DCS106 = ErrorResponse("DCS106", "Error initialising data admin", 500)
  val DCS107 = ErrorResponse("DCS107", "Error loading / retrieving data source info", 500)
  val DCS108 = ErrorResponse("DCS108", "Requested entity is not available", 400)

  /*
    Service related errors
   */
  val DCS201 = ErrorResponse("DCS201", "Service currently unavailable", 503)

  /*
    Flow related errors
   */
  val DCS301 = ErrorResponse("DCS301", "Requested entity is not available", 400)
  val DCS302 = ErrorResponse("DCS302", "Access to flow entity is unauthorized", 401)
  val DCS303 = ErrorResponse("DCS303", "Access to flow entity is forbidden", 403)
  val DCS304 = ErrorResponse("DCS304", "Flow entity could not be found", 404)
  val DCS305 = ErrorResponse("DCS305", "Inavlid request to process flow entity", 409)

  /*
    Web client errors
   */
  val DCS400 = ErrorResponse("DCS400", "Request is malformed", 400)

  /*
  Security errors
 */
  val DCS500 = ErrorResponse("DCS500", "Unkown authorisation error", 500)
  val DCS501 = ErrorResponse("DCS501", "Access token is missing", 401)
  val DCS502 = ErrorResponse("DCS502", "Access token is invalid", 401)
  val DCS503 = ErrorResponse("DCS503", "Requested action is no authorised", 401)

}


