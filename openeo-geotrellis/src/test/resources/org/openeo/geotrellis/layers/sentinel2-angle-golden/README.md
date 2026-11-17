# Golden per-pixel angles (issue #916, tier 1)

`golden.csv` holds sun and B05 viewing angles at 93 points of the two L2A fixtures in the parent directory:
`T31UFS` (B05 view zenith 1.9–10.8°) and `T31UGS` (crosses the swath centre, view zenith down to 1.0°). The values
come from `generate_golden.py`, an independent NumPy implementation of the method, written from its description
rather than from the Scala code. Detector labels are read with GDAL (rasterio) from the native JP2 masks.

Regenerate with numpy and rasterio installed (the output is deterministic):

```
python generate_golden.py
```

Columns:

| Column | Meaning |
| --- | --- |
| `tile` | `T31UFS` or `T31UGS` |
| `kind` | Why the point was chosen: `random`, `detector_edge` (both sides of a detector boundary), `pixel_edge` (exactly on a mask pixel edge), `grid_node` (exactly on an angle-grid node), `filled_node` (interpolation uses plane-filled nodes), `no_detector` (mask label 0), `outside_tile`, `near_nadir` |
| `x`, `y` | Position in the tile CRS (EPSG:32631), in metres; multiples of 1/8 m |
| `label` | B05 detector label at the point (0: none) |
| `sun_zenith`, `sun_azimuth` | Sun angles in degrees, `NaN` for NoData |
| `view_zenith`, `view_azimuth` | B05 viewing angles in degrees, `NaN` for NoData |

`SentinelAngleGoldenTest` compares the Scala results to these values: at most 0.0001° for zenith angles, sun
azimuth and the angle between viewing directions, NoData at the same points, and identical detector labels.

The fixtures contain Copernicus Sentinel data (2022); see the README of each `.SAFE` directory.
