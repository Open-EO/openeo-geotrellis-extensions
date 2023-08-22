import xarray
from openeo.udf import XarrayDataCube


def convert_dimensions(metadata, context:dict):
    from openeo.metadata import CollectionMetadata
    new_metadata = {
          "x": {"type": "spatial", "axis": "x", "step": 0.3, "reference_system": 4326},
          "y": {"type": "spatial", "axis": "y", "step": 0.3, "reference_system": 4326},
          "t": {"type": "temporal"}
    }
    return CollectionMetadata(new_metadata)

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array: xarray.DataArray = cube.get_array()
    array += 60
    return XarrayDataCube(array)