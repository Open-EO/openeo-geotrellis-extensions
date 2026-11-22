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

The bundled EGM96 geoid is used by default, including when packaged in a JAR.
Supply a custom `geoidSourceFactory` to `TerrainCorrectionProcessor` to override it.
Explicitly passing `None`
still causes `openScene` to fail: terrain correction requires a geoid.

## Terrain flattening (gamma0) algorithms

For `coefficient = "gamma0-terrain"` the algorithm is selected with the `options` of `sar_backscatter`
(or, as a default, the environment variable `S1_RTC_ALGORITHM`). Sigma0 is unaffected.

| `rtc_algorithm`         | Method | Cost |
|-------------------------|--------|------|
| `angle` (default)       | Per pixel `σ0 · tan θ_loc / sin θ_el`, from the DEM-derived local incidence angle. Vollrath et al. (2020), after Ulander (1996) and Hoekman & Reiche (2015). Unchanged. | low |
| `bilinear_distribution` | Area-based: DEM facets are projected into radar geometry and their gamma area is summed per radar pixel; `γ0 = β0 / ANF` (β0 from the `betaNought` calibration LUT). Port of ISCE3 `computeRtcBilinearDistribution`; the method of Small (2011) as used by OPERA RTC-S1 / NISAR. Accurate on steep slopes and in layover. | high (about 10-20x the geometry work of `angle`) |
| `auto`                  | Per tile: if no pixel has a local incidence angle outside `[rtc_auto_min_local_incidence, rtc_auto_max_local_incidence]` the tile is computed with `angle` only. If at least `rtc_auto_tile_fraction` of the pixels are steep, the whole tile uses `bilinear_distribution`. Otherwise it is used only for steep pixels and their neighbourhood (`rtc_auto_dilation_pixels`), `angle` elsewhere. | in between |

More options (only for the area-based algorithms unless noted):

| option                         | default | |
|--------------------------------|---------|-|
| `rtc_dem_upsampling`           | 2       | DEM facets per output pixel, per axis (ISCE3 `dem_upsampling`) |
| `rtc_dem_margin_pixels`        | 20      | DEM margin around the tile, in output pixels |
| `rtc_min_value_db`             | none    | pixels with a normalisation factor below this become NaN (ISCE3 `rtc_min_value_db`) |
| `rtc_auto_min_local_incidence` | 20      | degrees, `auto` only |
| `rtc_auto_max_local_incidence` | 70      | degrees, `auto` only |
| `rtc_auto_dilation_pixels`     | 2       | `auto` only |
| `rtc_auto_tile_fraction`       | 0.02    | `auto` only, fraction of steep pixels above which the whole tile is area-based |
| `rtc_method_band`              | false   | appends a band with the method used per pixel: 0 = angle, 1 = area-based. Useful to assess `auto` |

Not ported yet: ISCE3's area projection (`computeRtcAreaProj`, Shiroma et al. 2022).

### References

- Vollrath, A., Mullissa, A., Reiche, J. (2020). Angular-Based Radiometric Slope Correction for Sentinel-1 on Google Earth Engine. *Remote Sensing* 12(11), 1867. doi:10.3390/rs12111867
- Hoekman, D.H., Reiche, J. (2015). Multi-model radiometric slope correction of SAR images of complex terrain using a two-stage semi-empirical approach. *Remote Sensing of Environment* 156, 1-10. doi:10.1016/j.rse.2014.08.037
- Ulander, L.M.H. (1996). Radiometric slope correction of synthetic-aperture radar images. *IEEE TGRS* 34(5), 1115-1122. doi:10.1109/36.536527
- Small, D. (2011). Flattening Gamma: Radiometric Terrain Correction for SAR Imagery. *IEEE TGRS* 49(8), 3081-3093. doi:10.1109/TGRS.2011.2120616
- Shiroma, G.H.X., Lavalle, M., Buckley, S.M. (2022). An Area-Based Projection Algorithm for SAR Radiometric Terrain Correction and Geocoding. *IEEE TGRS* 60, 5222723. doi:10.1109/TGRS.2022.3147472
- ISCE3, `cxx/isce3/geometry/RTC.cpp`, Caltech/JPL, Apache-2.0: https://github.com/isce-framework/isce3
