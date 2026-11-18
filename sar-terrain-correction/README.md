# sar-terrain-correction

A small Scala framework that produces a calibrated, terrain-corrected output tile
(sigma0 + local incidence angle) from a Sentinel-1 GRD product, given:

* a target `Extent`, `CellSize` and `CRS`
* a STAC item URL pointing at the GRD (e.g. CDSE STAC API)

It exposes a `TerrainCorrectionBackend`:

| Backend  | Class                                                  | Notes |
|----------|--------------------------------------------------------|-------|
| Native   | `org.openeo.sar.backend.native.NativeBackend`          | Pure Scala, no deps beyond GeoTrellis |

The backends accept the `TileComputeContext` (assembled metadata + raster
sources) and produce the `MultibandTile` layout:

```
band 0: sigma0 (linear, Float32, NaN where outside swath / invalid)
band 1: localIncidenceAngle (degrees, Float32)
band 2: validity mask       (0 = invalid, 1 = valid)
```

This module is single-tile: no Spark. It is designed to be wrapped in a
Spark job that iterates over output tile keys.

## Reading SAFE bytes from S3

All raster I/O goes through GeoTrellis `RasterSource`, which already supports
S3 (`s3://...`) and HTTP(S) URLs. No SNAP, no FUSE mount, no SAFE-zip reader
needed; we read the measurement TIFFs and DEM COGs directly.

## Elevation model

The DEM is selected with `elevation_model` of `sar_backscatter`
(`SarBackscatterParameters.elevationModel`):

- absent (`null`): the back-end default, `COPERNICUS_30`;
- the string `"null"`: no DEM, terrain height is 0 m (the geoid is still applied);
- `COPERNICUS_30`, or the URL of a STAC collection
  (`https://<host>/<root>/collections/<id>`): the DEM rasters are the items of
  that collection; `options["dem_asset"]` sets the asset key (default `data`);
- anything else is rejected.

The collection is searched once per request on the driver (overall extent,
split at the antimeridian); each tile then reads only the overlapping items,
mosaicked at the finest resolution with bilinear resampling. Height is 0 m
where no item exists (sea, coast) or the DEM has nodata. If items exist but
cannot be read, the DEM is NaN and the soft error counter is increased, unless
soft errors are disabled for the load, in which case the read fails.

A geoid is mandatory: `openScene` fails without one (`S1_GEOID_TIFF_URI` for the
default provider).
