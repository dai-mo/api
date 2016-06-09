package org.dcs.api.error

/**
  * Created by cmathew on 05/06/16.
  */

case class ErrorResponse(code: String,
                         message: String,
                         httpStatusCode: Int,
                         var errorMessage: String = "") {
  def withErrorMessage(em: String): ErrorResponse  = {
    this.errorMessage = em
    this
  }
}

class RESTException(val errorResponse: ErrorResponse)
  extends Exception(errorResponse.code + ":" +
    errorResponse.httpStatusCode + ":" +
    errorResponse.message)
