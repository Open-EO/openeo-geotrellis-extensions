"""Generate golden per-pixel Sentinel-2 angles for the issue #916 tier-1 tests.

An independent NumPy implementation of the method in the issue #916 plan (section 5), written from that description
rather than from the Scala code. It reads the two L2A fixtures next to this directory and writes `golden.csv` with
about 50 points per tile:

    python generate_golden.py

Needs numpy and rasterio (GDAL reads the JP2 detector masks). The output is deterministic.

Method:
- Grid position: col = (x - ULX) / COL_STEP and row = (ULY - y) / ROW_STEP; node (0, 0) is the tile's upper-left
  corner. Points outside the 10 m tile extent are NaN.
- Sun: bilinear interpolation of zenith and of azimuth, the azimuths unwrapped around one valid corner. NaN if a
  corner with non-zero weight is missing.
- Detector: label of the native 20 m B05 mask pixel containing the point (floor at pixel edges); 0 is NaN.
- View: per detector, look components u = tan(zen) sin(az), v = tan(zen) cos(az) at valid nodes, missing nodes filled
  from least-squares planes a + b col + c row (at least 3 non-collinear valid nodes). Nodes within 2 rings
  (Chebyshev distance) of a valid node are usable. Bilinear interpolation of u and v, NaN if a corner with non-zero
  weight is not usable; zen = atan(hypot(u, v)), az = atan2(u, v) mod 360.
"""

import csv
import math
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import rasterio

HERE = Path(__file__).resolve().parent
FIXTURES = {
    "T31UFS": HERE.parent
    / "S2B_MSIL2A_20220502T103619_N0400_R008_T31UFS_20220502T123919.SAFE/GRANULE/L2A_T31UFS_A026918_20220502T103700",
    "T31UGS": HERE.parent
    / "S2B_MSIL2A_20220502T103619_N0400_R008_T31UGS_20220502T123919.SAFE/GRANULE/L2A_T31UGS_A026918_20220502T103700",
}
BAND_ID = "4"  # B05
MASK_RESOLUTION = 20.0
FILL_RINGS = 2
COLUMNS = [
    "tile",
    "kind",
    "x",
    "y",
    "label",
    "sun_zenith",
    "sun_azimuth",
    "view_zenith",
    "view_azimuth",
]


def _local(element):
    return re.sub(r"^\{.*\}", "", element.tag)


def _find(element, *path):
    for name in path:
        element = next(child for child in element if _local(child) == name)
    return element


def _children(element, name):
    return [child for child in element if _local(child) == name]


def _text_of(element):
    if element.text is None:
        raise ValueError(f"empty element {_local(element)}")
    return element.text


def _attribute(element, name):
    value = element.get(name)
    if value is None:
        raise ValueError(f"element {_local(element)} has no attribute {name}")
    return value


def _values(element):
    rows = [
        [float(token) for token in _text_of(row).split()]
        for row in _children(_find(element, "Values_List"), "VALUES")
    ]
    return np.array(rows)  # float() parses "NaN"; non-finite values are missing


def _grid(element):
    zenith, azimuth = _find(element, "Zenith"), _find(element, "Azimuth")
    return {
        "col_step": float(_text_of(_find(zenith, "COL_STEP"))),
        "row_step": float(_text_of(_find(zenith, "ROW_STEP"))),
        "zenith": _values(zenith),
        "azimuth": _values(azimuth),
    }


def read_tile(xml_path):
    root = ET.parse(xml_path).getroot()
    geocoding = _find(root, "Geometric_Info", "Tile_Geocoding")
    size = next(e for e in _children(geocoding, "Size") if e.get("resolution") == "10")
    position = next(
        e for e in _children(geocoding, "Geoposition") if e.get("resolution") == "10"
    )
    ulx, uly = float(_text_of(_find(position, "ULX"))), float(
        _text_of(_find(position, "ULY"))
    )
    width, height = 10 * int(_text_of(_find(size, "NCOLS"))), 10 * int(
        _text_of(_find(size, "NROWS"))
    )
    angles = _find(root, "Geometric_Info", "Tile_Angles")
    view = {
        int(_attribute(g, "detectorId")): _grid(g)
        for g in _children(angles, "Viewing_Incidence_Angles_Grids")
        if g.get("bandId") == BAND_ID
    }
    return {
        "extent": (ulx, uly - height, ulx + width, uly),
        "sun": _grid(_find(angles, "Sun_Angles_Grid")),
        "view": view,
    }


def fill_view_grid(grid):
    """Look components with missing nodes filled from planes, and the usable-node mask; None without a plane."""
    zenith, azimuth = grid["zenith"], grid["azimuth"]
    valid = np.isfinite(zenith) & np.isfinite(azimuth)
    tan_zenith = np.tan(np.radians(np.where(valid, zenith, 0.0)))
    u = tan_zenith * np.sin(np.radians(np.where(valid, azimuth, 0.0)))
    v = tan_zenith * np.cos(np.radians(np.where(valid, azimuth, 0.0)))
    rows, cols = np.nonzero(valid)
    design = np.column_stack([np.ones(len(rows)), cols, rows]).astype(float)
    if len(rows) < 3 or np.linalg.matrix_rank(design) < 3:
        return None
    plane_u = np.linalg.lstsq(design, u[valid], rcond=None)[0]
    plane_v = np.linalg.lstsq(design, v[valid], rcond=None)[0]
    all_rows, all_cols = np.indices(valid.shape)
    u = np.where(valid, u, plane_u[0] + plane_u[1] * all_cols + plane_u[2] * all_rows)
    v = np.where(valid, v, plane_v[0] + plane_v[1] * all_cols + plane_v[2] * all_rows)
    distance = np.full(valid.shape, np.inf)
    for r, c in zip(rows, cols):
        distance = np.minimum(
            distance, np.maximum(np.abs(all_rows - r), np.abs(all_cols - c))
        )
    return {**grid, "u": u, "v": v, "usable": distance <= FILL_RINGS, "valid": valid}


def _cell(grid, x, y, extent):
    """Upper-left node (i, j) and fractions (fx, fy) of the cell holding (x, y), or None outside tile or grid."""
    xmin, ymin, xmax, ymax = extent
    if not (xmin <= x <= xmax and ymin <= y <= ymax):
        return None
    col, row = (x - xmin) / grid["col_step"], (ymax - y) / grid["row_step"]
    n_rows, n_cols = grid["zenith"].shape
    if not (0 <= col <= n_cols - 1 and 0 <= row <= n_rows - 1):
        return None
    i, j = min(math.floor(col), n_cols - 2), min(math.floor(row), n_rows - 2)
    return i, j, col - i, row - j


def _corners(i, j, fx, fy):
    """(row, col, weight) of the four corners with non-zero weight."""
    corners = [
        (j, i, (1 - fx) * (1 - fy)),
        (j, i + 1, fx * (1 - fy)),
        (j + 1, i, (1 - fx) * fy),
        (j + 1, i + 1, fx * fy),
    ]
    return [corner for corner in corners if corner[2] != 0]


def sun_angles(sun, x, y, extent):
    cell = _cell(sun, x, y, extent)
    if cell is None:
        return math.nan, math.nan
    corners = _corners(*cell)
    zen, az = sun["zenith"], sun["azimuth"]
    if not all(np.isfinite(zen[r, c]) and np.isfinite(az[r, c]) for r, c, _ in corners):
        return math.nan, math.nan
    zenith = sum(w * zen[r, c] for r, c, w in corners)
    reference = az[corners[0][0], corners[0][1]]
    azimuth = sum(
        w * (reference + (az[r, c] - reference + 180) % 360 - 180)
        for r, c, w in corners
    )
    return zenith, azimuth % 360


def view_angles(filled, x, y, extent):
    """Viewing (zenith, azimuth, uses_filled_node) at (x, y) from one detector's filled grid."""
    if filled is None:
        return math.nan, math.nan, False
    cell = _cell(filled, x, y, extent)
    if cell is None:
        return math.nan, math.nan, False
    corners = _corners(*cell)
    if not all(filled["usable"][r, c] for r, c, _ in corners):
        return math.nan, math.nan, True
    u = sum(w * filled["u"][r, c] for r, c, w in corners)
    v = sum(w * filled["v"][r, c] for r, c, w in corners)
    uses_filled = not all(filled["valid"][r, c] for r, c, _ in corners)
    return (
        math.degrees(math.atan(math.hypot(u, v))),
        math.degrees(math.atan2(u, v)) % 360,
        uses_filled,
    )


def label_at(mask, x, y, extent):
    col = math.floor((x - extent[0]) / MASK_RESOLUTION)
    row = math.floor((extent[3] - y) / MASK_RESOLUTION)
    if not (0 <= row < mask.shape[0] and 0 <= col < mask.shape[1]):
        return 0
    return int(mask[row, col])


def _quantized(value):
    """Round to 1/8 m, so that coordinates and pixel centres around them are exact in binary."""
    return round(value * 8) / 8


def select_points(tile, mask, filled, rng):
    xmin, ymin, xmax, ymax = tile["extent"]
    points = []

    def add(kind, x, y):
        points.append((kind, float(x), float(y)))

    for _ in range(20):
        add(
            "random",
            _quantized(rng.uniform(xmin, xmax)),
            _quantized(rng.uniform(ymin, ymax)),
        )

    # Both sides of detector boundaries between east-west neighbouring 20 m mask pixels.
    left, right = mask[:, :-1], mask[:, 1:]
    boundary_rows, boundary_cols = np.nonzero(
        (left != right) & (left > 0) & (right > 0)
    )
    for k in rng.choice(len(boundary_rows), size=6, replace=False):
        row, col = int(boundary_rows[k]), int(boundary_cols[k])
        y = _quantized(ymax - MASK_RESOLUTION * (row + rng.uniform(0.1, 0.9)))
        for side in (col, col + 1):
            add(
                "detector_edge",
                _quantized(xmin + MASK_RESOLUTION * (side + rng.uniform(0.1, 0.9))),
                y,
            )
    # Exactly on a pixel's west and north edges: floor puts the point in that pixel.
    row, col = int(boundary_rows[0]), int(boundary_cols[0]) + 1
    add(
        "pixel_edge", xmin + MASK_RESOLUTION * col, ymax - MASK_RESOLUTION * (row + 0.5)
    )
    add(
        "pixel_edge", xmin + MASK_RESOLUTION * (col + 0.5), ymax - MASK_RESOLUTION * row
    )

    # Exactly on grid nodes, where only one corner has a non-zero weight.
    step = tile["sun"]["col_step"]
    add("grid_node", xmin + 5 * step, ymax - 7 * step)
    add("grid_node", xmin + 12 * step, ymax - 3 * step)

    # Points whose interpolation uses filled nodes, or nodes beyond the fill range.
    candidates = {"filled_node": [], "beyond_fill": []}
    for _ in range(40000):
        x, y = _quantized(rng.uniform(xmin, xmax)), _quantized(rng.uniform(ymin, ymax))
        label = label_at(mask, x, y, tile["extent"])
        if label == 0:
            continue
        zenith, _, uses_filled = view_angles(filled.get(label), x, y, tile["extent"])
        if uses_filled:
            candidates["beyond_fill" if math.isnan(zenith) else "filled_node"].append(
                (x, y)
            )
    for kind, wanted in (("filled_node", 6), ("beyond_fill", 3)):
        for x, y in candidates[kind][:wanted]:
            add(kind, x, y)

    no_detector_rows, no_detector_cols = np.nonzero(mask == 0)
    for k in range(min(2, len(no_detector_rows))):
        index = int(rng.integers(len(no_detector_rows)))
        add(
            "no_detector",
            xmin + MASK_RESOLUTION * (no_detector_cols[index] + 0.5),
            ymax - MASK_RESOLUTION * (no_detector_rows[index] + 0.5),
        )

    add("outside_tile", xmin - 100.0, ymax - 1000.0)

    # Around the lowest viewing zenith on the grid, where the look direction is closest to nadir.
    lowest = min(
        (
            np.nanmin(grid["zenith"]),
            detector,
            np.unravel_index(np.nanargmin(grid["zenith"]), grid["zenith"].shape),
        )
        for detector, grid in tile["view"].items()
    )
    if lowest[0] < 1.0:
        row, col = lowest[2]
        for _ in range(5):
            add(
                "near_nadir",
                _quantized(xmin + step * (col + rng.uniform(-1, 1))),
                _quantized(ymax - step * (row + rng.uniform(-1, 1))),
            )
    return points


def _text(value):
    return "NaN" if math.isnan(value) else repr(float(value))


def main():
    rows = []
    for index, (name, granule) in enumerate(FIXTURES.items()):
        tile = read_tile(granule / "MTD_TL.xml")
        with rasterio.open(granule / "QI_DATA/MSK_DETFOO_B05.jp2") as dataset:
            mask = dataset.read(1)  # native resolution, never an overview
        filled = {
            detector: fill_view_grid(grid) for detector, grid in tile["view"].items()
        }
        rng = np.random.default_rng(916 + index)
        for kind, x, y in select_points(tile, mask, filled, rng):
            label = label_at(mask, x, y, tile["extent"])
            sun_zenith, sun_azimuth = sun_angles(tile["sun"], x, y, tile["extent"])
            if label == 0:
                view_zenith = view_azimuth = math.nan
            else:
                view_zenith, view_azimuth, _ = view_angles(
                    filled.get(label), x, y, tile["extent"]
                )
            rows.append(
                [name, kind, _text(x), _text(y), str(label)]
                + [
                    _text(value)
                    for value in (sun_zenith, sun_azimuth, view_zenith, view_azimuth)
                ]
            )
    with open(HERE / "golden.csv", "w", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows(rows)
    print(f"wrote {len(rows)} points to {HERE / 'golden.csv'}")


if __name__ == "__main__":
    main()
