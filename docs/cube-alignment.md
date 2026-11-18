# How to optimally load datacubes for various datasets

## Sentinel-2
Sentinel-2 data is pretty well behaved:
* each tile (e.g. 31UFS) has a fixed bounding box
* pixel coordinates are rounded to 10m so always end with a 0 as last integer digit

## Sentinel-1 Sigma-0 Terrascope
* not sure if bounding boxes follow a convention
* pixel coordinates are not aligned to 10m, and not to Sentinel-2.
    * Not sure how this works with original data? Perhaps SNAP shifted pixels
    
## Alignment strategies
ZoomedLayoutScheme (LatLon, Webmercator)
-> always resample/reproject because never aligned with native data

FloatingLayoutscheme + maxresolution
Indicates that we are loading data in some native form?
But do we immediately align pixels to Sentinel-2?
openEO anyway requires an explicit resample, so maybe better to stick with native layout unless
requested otherwise.


## Optimization
Given that we can determine:
- target resolution
- target boundingbox
- target CRS

Questions:
- is there impact on target extent larger than original bounds?
- pixel align to sentinel-2 convention, which is rounded to the CellSize?
    - gdal can do this without having to specify target extent, but we don't have gdal 3.1 yet.    