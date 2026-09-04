# sar-terrain-correction

A small Scala framework that produces a calibrated, terrain-corrected output tile
(sigma0 + local incidence angle) from a Sentinel-1 GRD product, given:

* a target `Extent`, `CellSize` and `CRS`
* a STAC item URL pointing at the GRD (e.g. CDSE STAC API)

It exposes two interchangeable backends behind `TerrainCorrectionBackend`:

| Backend  | Class                                                  | Notes |
|----------|--------------------------------------------------------|-------|
| Native   | `org.openeo.sar.backend.native.NativeBackend`          | Pure Scala, no deps beyond GeoTrellis |
| ONNX     | `org.openeo.sar.backend.onnx.OnnxBackend`              | Loads `sar_tc.onnx`, runs via ONNX Runtime (CPU/GPU) |

Both backends accept the same `TileComputeContext` (assembled metadata + raster
sources) and produce the same `MultibandTile` layout:

```
band 0: sigma0 (linear, Float32, NaN where outside swath / invalid)
band 1: localIncidenceAngle (degrees, Float32)
band 2: validity mask       (0 = invalid, 1 = valid)
```

This module is single-tile: no Spark. It is designed to be wrapped in a
Spark job that iterates over output tile keys.

## ONNX model

`scripts/build_onnx_model.py` builds `sar_tc.onnx` using PyTorch. Run once:

```
pip install torch onnx onnxruntime numpy
python scripts/build_onnx_model.py --out src/main/resources/sar_tc.onnx
```

The model expresses the per-pixel inner loop (zero-Doppler iteration, slant
range, bilinear sampling, calibration, incidence angle) as a single graph.

## Reading SAFE bytes from S3

All raster I/O goes through GeoTrellis `RasterSource`, which already supports
S3 (`s3://...`) and HTTP(S) URLs. No SNAP, no FUSE mount, no SAFE-zip reader
needed; we read the measurement TIFFs and DEM COGs directly.
