import xarray
from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array: xarray.DataArray = cube.get_array()
    array += 60
    return XarrayDataCube(array)