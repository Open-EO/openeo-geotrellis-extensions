package org.openeo.geotrelliscommon

import java.nio.file.Path

trait AuxiliaryFileWriter {
  def write(batchJobId: Option[String]): Path
}
