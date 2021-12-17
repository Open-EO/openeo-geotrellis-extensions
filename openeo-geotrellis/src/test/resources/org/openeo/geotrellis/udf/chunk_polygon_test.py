import xarray
from openeo.udf import XarrayDataCube

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array: xarray.DataArray = cube.get_array()
    array += 1000
    # Shape (#dates, #bands, #rows, #cols)
    array.loc[:, 'B01'] += 1
    array.loc[:, 'B02'] += 2
    return XarrayDataCube(array)
