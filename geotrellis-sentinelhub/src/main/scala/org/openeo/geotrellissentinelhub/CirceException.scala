package org.openeo.geotrellissentinelhub

import io.circe.{Decoder, DecodingFailure, ParsingFailure}
import io.circe.parser.{decode => circeDecode}
import cats.syntax.either._

/**
 * Restores error context deliberately removed by Circe (https://github.com/circe/circe/issues/288) by wrapping a Circe
 * error in an exception that includes a stack trace.
 */
class CirceException(message: String, cause: Throwable) extends RuntimeException(message, cause)

object CirceException {
  // TODO: move this somewhere else?
  def decode[A: Decoder](json: String): Either[CirceException, A] =
    circeDecode[A](json).leftMap {
      case e: ParsingFailure => e.initCause(e.underlying); new CirceException(s"failed to parse $json", e)
      case e: DecodingFailure => new CirceException(s"failed to decode $json", e)
    }
}
