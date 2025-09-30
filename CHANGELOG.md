# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add support for reading ZStd compressed GTiff files ([#516](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/516))
- `saveRDDTemporalAllowAssetPerBand`: add support for overviews ([openeo-geopyspark-driver#1151](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1151))

### Changed

- Upgrade Geotrellis library to 3.8.0+9-dbaac792-SNAPSHOT. This may impact code using kernels (e.g. in SCL dilation masks) in a positive way (more accurate).
  Cfr. Geotrellis [changelog](https://github.com/locationtech/geotrellis/blob/master/CHANGELOG.md)
- aggregate_temporal: performance improvement for 2 cases. This tries to avoid empty or small partitions, 
  which also results in increased partition sizes, thus has a memory impact. ([#445](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/445))
- `resample_cube_spatial`: for major resolution increase, rearrange datacube. ([#523](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/523))
- `load_collection`/`load_stac`: reduce datacube chunck size for small extents ([#523](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/523))

### Removed

### Fixed

- Fix thresholds in `linear_scale_range` to trigger type casting ([openeo-geopyspark-driver#1275](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1275))
- Major performance improvement for load_collection/load_stac of (very) sparse datacubes   ([#465](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/465))
- load_collection/load_stac: reduce number of tasks and thus resource use by making partitioner settings adaptive
- `load_stac`: avoid areas of zeroes for assets with integral values that don't define NODATA ([#446](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/446))