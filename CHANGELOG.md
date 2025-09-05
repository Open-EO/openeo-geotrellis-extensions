# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add support for reading ZStd compressed GTiff files

### Changed

- Upgrade Geotrellis library to 3.8.0+9-dbaac792-SNAPSHOT. This may impact kernels (e.g. in SCL dilation masks) in a positive way (more accurate).

### Removed

### Fixed

- Fix thresholds in `linear_scale_range` to trigger type casting ([openeo-geopyspark-driver#1275](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1275))
- Major performance improvement for load_collection/load_stac of (very) sparse datacubes   ([#465](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/465))
- load_collection/load_stac: reduce number of tasks and thus resource use by making partitioner settings adaptive
