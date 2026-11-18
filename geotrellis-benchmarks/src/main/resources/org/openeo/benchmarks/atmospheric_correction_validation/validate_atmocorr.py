# -*- coding: utf-8 -*-

import openeo
import logging
import os
from openeo.rest.datacube import THIS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

openeo_url='http://openeo-dev.vgt.vito.be/openeo/1.0.0/'
#openeo_url='http://localhost:8080/openeo/1.0.0/'

openeo_user=os.environ.get('OPENEO_USER','USERNAME_HERE')
openeo_pass=os.environ.get('OPENEO_PASS','PASSWORD_HERE')

# job_options={
#     'driver-memory':'4G',
#     'executor-memory':'4G',
#     'profile':'true'
# }

layerid="SENTINEL2_L1C_SENTINELHUB"
startdate='2019-04-11'
enddate='2019-04-11'
bbox=(655000,5674000,670000,5685000)

bands=None

def getImageCollection(eoconn,layerid,startdate,enddate,bands,bbox):
    coll=eoconn.load_collection(layerid)\
    .filter_temporal(startdate,enddate)\
    .filter_bbox(crs="EPSG:32631", **dict(zip(["west", "south", "east", "north"], bbox)))
    if bands is not None: coll=coll.filter_bands(bands)
    return coll

if __name__ == '__main__':

    eoconn=openeo.connect(openeo_url,default_timeout=1000000)
    eoconn.authenticate_basic(openeo_user,openeo_pass)

    # TODO: water and cloud mask integration (watermask needs work on backend side)

    sentinel2data=getImageCollection(eoconn, layerid, startdate, enddate, bands, bbox)
    sentinel2data=sentinel2data.process(
        process_id="atmospheric_correction",
        arguments={
            "data": THIS,
#            "missionId": "SENTINEL2",
#            "sza" : 43.5,
#            "vza" : 6.96,
#            "raa" : 117.,
#            "gnd" : 0.1,
#            "aot" : 0.2,
#            "cwv" : 2.0,
            "appendDebugBands" : 1
        }
    )
    sentinel2data.download('output_openeo_'+startdate+'.nc',format='netcdf')

    print("FINISHED")







