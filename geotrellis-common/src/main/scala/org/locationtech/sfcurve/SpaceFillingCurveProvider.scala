/***********************************************************************
 * Copyright (c) 2015 Azavea.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 * Taken from the now defunct https://github.com/locationtech/sfcurve
 ***********************************************************************/

package org.locationtech.sfcurve

trait SpaceFillingCurveProvider {
  def canProvide(name: String): Boolean
  def build2DSFC(args: Map[String, java.io.Serializable]): SpaceFillingCurve2D
}
