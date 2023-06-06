package org.openeo.geotrelliscommon

import geotrellis.macros.{DoubleTileMapper, DoubleTileVisitor, IntTileMapper, IntTileVisitor}
import geotrellis.raster.{ByteCellType, CellType, DoubleArrayTile, DoubleCellType, FloatCellType, IntArrayTile, IntCellType, NODATA}
import geotrellis.raster.testkit.{RasterMatchers, TileBuilders}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ResampledTileTest extends AnyFunSpec with Matchers {

  describe("Get should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)

    it("0,0 => 0,0") {
      resampledTile.get(0, 0) should be(sourceTile.get(0, 0))
    }
    it("3,3 => 1,1") {
      resampledTile.get(3, 3) should be(sourceTile.get(1, 1))
    }
    it("2,3 => 1,1") {
      resampledTile.get(2, 3) should be(sourceTile.get(1, 1))
    }
    it("1,3 => 0,1") {
      resampledTile.get(1, 3) should be(sourceTile.get(0, 1))
    }
  }

  describe("GetDouble should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)

    it("0,0 => 0,0") {
      resampledTile.getDouble(0, 0) should be(sourceTile.getDouble(0, 0))
    }
    it("3,3 => 1,1") {
      resampledTile.getDouble(3, 3).isNaN should be(true)
    }
    it("2,3 => 1,1") {
      resampledTile.getDouble(2, 3).isNaN should be(true)
    }
    it("1,3 => 0,1") {
      resampledTile.getDouble(1, 3) should be(sourceTile.getDouble(0, 1))
    }
  }

  describe("Converting to a different celltype should still return a ResampledTile") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)

    it("ByteCellType should be a ResampledTile") {
      resampledTile.convert(ByteCellType) should be(a[ResampledTile])
    }
    it("DoubleCellType should be a ResampledTile") {
      resampledTile.convert(DoubleCellType) should be(a[ResampledTile])
    }
    it("FloatCellType should be a ResampledTile") {
      resampledTile.convert(FloatCellType) should be(a[ResampledTile])
    }
  }

  describe("Map should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)

    it("mapDouble returns a ResampledTile") {
      resampledTile.mapDouble(_ + 1.0) should be(a[ResampledTile])
    }
    it("mapDouble 0,0 => 0,0") {
      resampledTile.mapDouble(_ + 1.0).getDouble(0, 0) should be(sourceTile.mapDouble(_ + 1.0).getDouble(0, 0))
    }
    it("mapDouble 3,3 => 1,1") {
      resampledTile.mapDouble(_ + 1.0).getDouble(3, 3).isNaN should be(true)
    }
    it("mapDouble 2,3 => 1,1") {
      resampledTile.mapDouble(_ + 1.0).getDouble(2, 3).isNaN should be(true)
    }
    it("mapDouble 1,3 => 0,1") {
      resampledTile.mapDouble(_ + 1.0).getDouble(1, 3) should be(sourceTile.mapDouble(_ + 1.0).getDouble(0, 1))
    }
    it("map returns a ResampledTile") {
      resampledTile.map(_ + 1) should be(a[ResampledTile])
    }
    it("map 0,0 => 0,0") {
      resampledTile.map(_ + 1).getDouble(0, 0) should be(sourceTile.map(_ + 1).getDouble(0, 0))
    }
    it("map 3,3 => 1,1") {
      resampledTile.map(_ + 1).getDouble(3, 3) should be(sourceTile.map(_ + 1).getDouble(1, 1))
    }
    it("map 2,3 => 1,1") {
      resampledTile.map(_ + 1).getDouble(2, 3) should be(sourceTile.map(_ + 1).getDouble(1, 1))
    }
    it("map 1,3 => 0,1") {
      resampledTile.map(_ + 1).getDouble(1, 3) should be(sourceTile.map(_ + 1).getDouble(0, 1))
    }
  }

  describe("Foreach should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    sourceTile.foreach(_ + 1)
    resampledTile.foreach(_ + 1)

    it("Foreach should still be resampled") {
      resampledTile should be(a[ResampledTile])
    }
    it("foreach 0,0 => 0,0") {
      resampledTile.getDouble(0, 0) should be(sourceTile.getDouble(0, 0))
    }
    it("foreach 3,3 => 1,1") {
      resampledTile.getDouble(3, 3).isNaN should be(true)
    }
    it("foreach 2,3 => 1,1") {
      resampledTile.getDouble(2, 3).isNaN should be(true)
    }
    it("foreach 1,3 => 0,1") {
      resampledTile.getDouble(1, 3) should be(sourceTile.getDouble(0, 1))
    }
  }

  describe("ForeachDouble should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    sourceTile.foreachDouble(_ + 1)
    resampledTile.foreachDouble(_ + 1)

    it("ForeachDouble should still be resampled") {
      resampledTile should be(a[ResampledTile])
    }
    it("foreachDouble 0,0 => 0,0") {
      resampledTile.getDouble(0, 0) should be(sourceTile.getDouble(0, 0))
    }
    it("foreachDouble 3,3 => 1,1") {
      resampledTile.getDouble(3, 3).isNaN should be(true)
    }
    it("foreachDouble 2,3 => 1,1") {
      resampledTile.getDouble(2, 3).isNaN should be(true)
    }
    it("foreachDouble 1,3 => 0,1") {
      resampledTile.getDouble(1, 3) should be(sourceTile.getDouble(0, 1))
    }
  }

  describe("Combine should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    val otherTile = DoubleArrayTile(Array(1.0, 2.0, 3.0, 4.0), 2, 2, DoubleCellType)
    val combinedTile = resampledTile.combine(otherTile) { (z1, z2) => z1 + z2 }

    it("Combine should still be resampled") {
      combinedTile should be(a[ResampledTile])
    }
    it("combineDouble 0,0 => 0,0") {
      combinedTile.get(0, 0) should be(sourceTile.get(0, 0) + otherTile.get(0, 0))
    }
    it("combineDouble 3,3 => 1,1") {
      combinedTile.get(3, 3) should be(sourceTile.get(1, 1) + otherTile.get(1, 1))
    }
    it("combineDouble 2,3 => 1,1") {
      combinedTile.get(2, 3) should be (sourceTile.get(1, 1) + otherTile.get(1, 1))
    }
    it("combineDouble 1,3 => 0,1") {
      combinedTile.get(1, 3) should be(sourceTile.get(0, 1) + otherTile.get(0, 1))
    }
  }

  describe("CombineDouble should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    val otherTile = DoubleArrayTile(Array(1.0, 2.0, 3.0, 4.0), 2, 2, DoubleCellType)
    val combinedTile = resampledTile.combineDouble(otherTile) { (z1, z2) => z1 + z2 }

    it("CombineDouble should still be resampled") {
      combinedTile should be(a[ResampledTile])
    }
    it("combineDouble 0,0 => 0,0") {
      combinedTile.getDouble(0, 0) should be(sourceTile.getDouble(0, 0) + otherTile.getDouble(0, 0))
    }
    it("combineDouble 3,3 => 1,1") {
      combinedTile.getDouble(3, 3).isNaN should be(true)
    }
    it("combineDouble 2,3 => 1,1") {
      combinedTile.getDouble(2, 3).isNaN should be(true)
    }
    it("combineDouble 1,3 => 0,1") {
      combinedTile.getDouble(1, 3) should be(sourceTile.getDouble(0, 1) + otherTile.getDouble(0, 1))
    }
  }

  describe("MapIntMapper should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0, 1, -1, NODATA)
    val sourceTile = IntArrayTile(arr, 2, 2, IntCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    val intTileMapper = new IntTileMapper {
      override def apply(col: Int, row: Int, z: Int): Int = z + 1
    }
    val mappedTile = resampledTile.mapIntMapper(intTileMapper)

    it("MapIntMapper should still be resampled") {
      mappedTile should be(a[ResampledTile])
    }
    it("mapIntMapper 0,0 => 0,0") {
      mappedTile.get(0, 0) should be(sourceTile.get(0, 0) + 1)
    }
    it("mapIntMapper 3,3 => 1,1") {
      mappedTile.get(3, 3) should be(sourceTile.get(1, 1) + 1)
    }
    it("mapIntMapper 2,3 => 1,1") {
      mappedTile.get(2, 3) should be(sourceTile.get(1, 1) + 1)
    }
    it("mapIntMapper 1,3 => 0,1") {
      mappedTile.get(1, 3) should be(sourceTile.get(0, 1) + 1)
    }
  }

  describe("mapDoubleMapper should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    val doubleTileMapper = new DoubleTileMapper {
      override def apply(col: Int, row: Int, z: Double): Double = z + 1
    }
    val mappedTile = resampledTile.mapDoubleMapper(doubleTileMapper)

    it("MapDoubleMapper should still be resampled") {
      mappedTile should be(a[ResampledTile])
    }
    it("mapDoubleMapper 0,0 => 0,0") {
      mappedTile.getDouble(0, 0) should be(sourceTile.getDouble(0, 0) + 1)
    }
    it("mapDoubleMapper 3,3 => 1,1") {
      mappedTile.getDouble(3, 3).isNaN should be(true)
    }
    it("mapDoubleMapper 2,3 => 1,1") {
      mappedTile.getDouble(2, 3).isNaN should be(true)
    }
    it("mapDoubleMapper 1,3 => 0,1") {
      mappedTile.getDouble(1, 3) should be(sourceTile.getDouble(0, 1) + 1)
    }
  }

  describe("foreachIntVisitor should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0, 1, -1, -5)
    val sourceTile = IntArrayTile(arr, 2, 2, IntCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    val mutableTile = resampledTile.mutable
    val intTileVisitor = new IntTileVisitor {
      override def apply(col: Int, row: Int, z: Int): Unit = {
        mutableTile.set(col, row, z + 1)
      }
    }
    resampledTile.foreachIntVisitor(intTileVisitor)

    it("foreachIntVisitor 0,0 => 0,0") {
      mutableTile.get(0, 0) should be(resampledTile.get(0, 0) + 1)
    }
    it("foreachIntVisitor 3,3 => 3,3") {
      mutableTile.get(3, 3) should be(resampledTile.get(3, 3) + 1)
    }
    it("foreachIntVisitor 2,3 => 2,3") {
      mutableTile.get(2, 3) should be(resampledTile.get(2, 3) + 1)
    }
    it("foreachIntVisitor 1,3 => 1,3") {
      mutableTile.get(1, 3) should be(resampledTile.get(1, 3) + 1)
    }
  }

  describe("foreachDoubleVisitor should match the source tile with a constant scale factor for the rows and columns.") {
    val arr = Array(0.0, 1.0, -1.0, Double.NaN)
    val sourceTile = DoubleArrayTile(arr, 2, 2, DoubleCellType)
    val resampledTile = ResampledTile(sourceTile, 2, 2, 4, 4)
    val mutableTile = resampledTile.mutable
    val doubleTileVisitor = new DoubleTileVisitor {
      override def apply(col: Int, row: Int, z: Double): Unit = {
        mutableTile.setDouble(col, row, z + 1)
      }
    }
    resampledTile.foreachDoubleVisitor(doubleTileVisitor)

    it("foreachDoubleVisitor 0,0 => 0,0") {
      mutableTile.getDouble(0, 0) should be(resampledTile.getDouble(0, 0) + 1)
    }
    it("foreachDoubleVisitor 3,3 => 3,3") {
      mutableTile.getDouble(3, 3).isNaN should be(true)
    }
    it("foreachDoubleVisitor 2,3 => 2,3") {
      mutableTile.getDouble(2, 3).isNaN should be(true)
    }
    it("foreachDoubleVisitor 1,3 => 1,3") {
      mutableTile.getDouble(1, 3) should be(resampledTile.getDouble(1, 3) + 1)
    }
  }
}
