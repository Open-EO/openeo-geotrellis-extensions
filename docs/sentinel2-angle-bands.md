# Sentinel-2 sun and viewing angle bands

Collections built from Sentinel-2 L1C or L2A products (in the SAFE layout) can offer angle bands. They are read
from each tile's metadata file, `MTD_TL.xml`, and configured in the layer catalogue through
`granule_metadata_band_map`, which maps band names to `granule_metadata##N`
([Open-EO/openeo-geotrellis-extensions#916](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/916)).

## Bands

| `N` | Usual band name | Values |
| --- | --- | --- |
| 0 | `sunAzimuthAngles` | Tile mean of the sun azimuth: one value per tile |
| 1 | `sunZenithAngles` | Tile mean of the sun zenith |
| 2 | `viewAzimuthMean` | Tile mean of the B05 viewing azimuth |
| 3 | `viewZenithMean` | Tile mean of the B05 viewing zenith |
| 4 | `sunAzimuthAnglesPerPixel` | Sun azimuth per pixel |
| 5 | `sunZenithAnglesPerPixel` | Sun zenith per pixel |
| 6 | `viewAzimuthAnglesPerPixel` | B05 viewing azimuth per pixel, from the detector that observed the pixel |
| 7 | `viewZenithAnglesPerPixel` | B05 viewing zenith per pixel, from the detector that observed the pixel |

Bands 0–3 are unchanged. Use bands 4–7 where the angles matter per pixel, for example for BRDF normalisation:
across one tile the sun zenith varies by about 1.1–1.3°, the sun azimuth by 1.6–2.8°, and the B05 tile-mean
viewing zenith is off by 2.4–4.1° (median) and up to 6.7°.

In the openEO driver, the map must list `granule_metadata##0` up to its highest index without gaps, because the
index used is the position among the sorted values (see the driver's `docs/layercatalog.md`).

## Conventions

- **Values:** float32 degrees; NaN is NoData. Zenith angles are 0–90°. Azimuths are 0–360°, clockwise from
  geographic north at the point; they are not corrected for grid convergence when the data are reprojected.
- **Where values are calculated:** at the centre of every output pixel, after reprojection and resampling. Angle
  rasters are never resampled, so values are never blended, and in particular never across detectors.
- **Angle grids:** the metadata gives angles on 23 × 23 nodes, 5 km apart. Node (0, 0) lies on the tile's
  upper-left corner, as stated by the Sentinel-2 Mission Performance Centre.
- **Sun angles:** bilinear interpolation of the sun angle grid, with azimuths unwrapped across north.
- **Viewing angles:** those of band B05 for all bands. Its viewing direction differs from those of the other
  bands by an almost constant 0.13–0.72°. For each pixel:
  - The detector is the label of the native 20 m `MSK_DETFOO_B05` pixel that contains the pixel centre.
    Label 0 (no detector) gives NoData.
  - Within that detector's grid, the viewing direction is interpolated bilinearly as its look-direction
    components tan(zenith)·sin(azimuth) and tan(zenith)·cos(azimuth). Nodes outside the detector's footprint
    are filled from a least-squares plane through its valid nodes.
  - Values that would use a filled node more than two nodes away from the detector's valid nodes are NoData.

## Supported products

- **Processing baseline 04.00 and later, including the reprocessed Collection-1 (05.xx):** these have JP2
  detector masks, and all bands work.
- **Earlier baselines, with GML detector masks:** the per-pixel viewing bands are float32 with NaN NoData,
  including in strict mode and when requested without a sun band, with one warning per product. The sun bands
  work; tile-mean viewing angles are never used as a substitute.
- **Mask locations:** the masks are found relative to the SAFE directory of `MTD_TL.xml`, which must be at
  `<product>.SAFE/GRANULE/<granule>/MTD_TL.xml`. That may be a local path; `/eodata/…` or `s3://eodata/…`, read
  from the `/eodata` mount, or through `/vsis3/` when `AWS_DIRECT=TRUE`; another `s3://` or `/vsis3/` path; or an
  `https://` URL. OData `Nodes(…)` URLs and the AWS `tiles/…/qi/` layout are not supported.
- **Errors:** a missing or unreadable mask, or a broken part of the metadata, is treated like a missing asset: a
  soft error when soft errors are allowed, otherwise a failure. Composite reads by geographic extent and by pixel
  bounds both preserve healthy bands and return float32/NaN NoData for failed viewing bands in soft mode;
  cancellation still propagates. An I/O failure while reading the metadata file follows the same policy for both
  tile-mean and per-pixel bands. Each metadata problem only affects the bands
  that use the broken part; for example, a broken sun grid leaves the viewing bands intact. Truncated or
  syntactically malformed XML makes all four per-pixel outputs unavailable, following the same policy; malformed
  XML and invalid mean values follow the same soft/strict policy for tile-mean bands. Soft metadata failures keep
  the affected bands float32 with NaN NoData, including when they are combined with integer imagery. Missing
  angle assets and angle-only soft metadata failures use the same float32/NaN placeholders; when the item has no
  feature grid and no metadata source supplies one, the requested target layout is used. These placeholders follow
  the normal all-NoData tile policy: an angle-only request whose every band is NoData has no tiles by default, and
  retaining all-NoData tiles preserves the float32/NaN cube. Mixed requests with healthy imagery still return
  those tiles. Strict failures include the metadata path and retain the original XML parser or number-format cause.
  The tile mean is never used as a substitute.

## Imagery alignment

When per-pixel angle bands are selected, GeoTIFF imagery served by `DefaultRasterSourceProvider` is aligned to
`featureExtentInLayout`, the same target CRS, resolution and pixel origin used by the angle and JP2 sources.
This keeps shared pixel bounds valid in normal reads and after per-product grouping. The shared region is passed
to the GeoTIFF reader at construction, without a post-construction resampling step that opens image headers.
The native-grid `noResampleOnRead` shortcut is not used for these mixed requests, because its pixel origin can
differ. Reflectance-only requests keep their existing grid and shortcuts. The imagery uses the configured
resampling method; angle values are still calculated at output pixel centres, without resampling or changing the
interpolation method.

## Memory use

Per-pixel angles are evaluated in blocks of at most 256 × 256 target pixels, including when per-product loading
requests a large union of windows. Coordinate, detector-label and interpolation buffers are bounded by that block;
view reads reuse one opened mask for the whole read and fetch only each block's native mask window.

For per-pixel bands, parsed metadata and filled viewing grids are cached by XML path in a bounded 64-entry cache
per JVM. Concurrent cold reads of one path share a successful load and the same warn-once set. Failed loads are
not cached, and evicted tiles can be parsed again.

The raster API still returns a complete float32 output array: 4 bytes per requested pixel, or about 460 MiB for one
10980 × 10980 band at 10 m. Native mask buffers, output copies, other bands and concurrent reads add to that cost.
Chunked evaluation is not a streaming output API and does not make arbitrarily large requests fit in a fixed heap.

## Accuracy

- **Against an independent implementation of the same method** (NumPy, at 93 points of two tiles, one of them
  crossing the swath centre): differences of at most 0.00001°, which is float32 rounding.
- **Estimated error of the method:** hiding the outer ring of each detector's valid grid nodes and predicting them
  from the rest gives errors of at most 0.0045° one node beyond the footprint, and 0.024° two nodes beyond.
  Inside footprints, interpolating the look components adds at most about 0.0003°. For comparison, a
  viewing-direction error of 0.01° changes the BRDF c-factor (Roy et al.) by at most about 0.005%.
- **Against a prototype that interpolates the angles instead of the components** (B04, a 10 km × 10 km area):
  agreement to within 0.0011° inside footprints and 0.0041° near footprint edges, for the same detector labels.
- **Not validated against ESA or SNAP output.** The grid placement follows the statement of the Sentinel-2
  Mission Performance Centre.

## Limitations

- Viewing angles are those of B05, not of each band.
- Pixels near a detector boundary may belong to a different detector in their own band than in B05.
- Where detectors overlap, the mask assigns each pixel to one detector, and only that detector's angles are used.
