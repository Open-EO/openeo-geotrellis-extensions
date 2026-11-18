import xarray
from openeo.udf import XarrayDataCube

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    inarr: xarray.DataArray = cube.get_array()
    B1 = inarr[0].loc['B01']
    B2 = inarr[1].loc['B02']
    first_date = inarr[0].coords['t'].values
    newarr = (B2-B1)/(B2+B1)
    newarr = newarr.expand_dims(dim='bands', axis=-3).assign_coords(bands=['ndvi'])
    newarr = newarr.expand_dims(dim='t', axis=-4).assign_coords(t=[first_date])
    return XarrayDataCube(newarr)