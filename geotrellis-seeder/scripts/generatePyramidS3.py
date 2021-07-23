import argparse
import boto3
import logging
import math
import os
from functools import partial


def setupLogger():

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.INFO)
    # avoid adding multiple handlers which would cause one message to be printed multiple times
    #logger.handlers[0] = logging.StreamHandler()

    logging.root = logger
    logging.Logger.manager.root = logger


def parseArguments():

    parser = argparse.ArgumentParser(prog='generatePyramid.py')

    parser.add_argument("--endpoint_url",type=str,help="S3 endpoint_url",required=True)
    parser.add_argument("--region",type=str,help="S3 region",required=True)
    parser.add_argument("--bucket",type=str,help="Bucket",required=True)
    parser.add_argument("--collection",type=str,help="Collection identifier",required=True)
    parser.add_argument("--date",type=str,help="Date formatted as YYYY-MM-DD",required=True)
    parser.add_argument("--bottomZoom",type=int,help="Bottom (most detailed) zoom level, generated in PNG RGB format, e.g. 9",required=True)
    parser.add_argument("--topZoom",type=int,help="Top zoom level of the pyramid, e.g. 1",required=True)
    parser.add_argument("--minLon",type=float,help="Min lon (X)",required=False,default=float('nan'))
    parser.add_argument("--maxLon",type=float,help="Max lon (X)",required=False,default=float('nan'))
    parser.add_argument("--minLat",type=float,help="Min lat (Y)",required=False,default=float('nan'))
    parser.add_argument("--maxLat",type=float,help="Max lat (Y)",required=False,default=float('nan'))
    parser.add_argument("--compress",help="Enable conversion to PNG8",dest='compress',action='store_true',default=True)
    parser.add_argument("--no-compress",help="Disable conversion to PNG8",dest='compress',action='store_false',default=False)
    parser.add_argument("--local",help="Run locally instead of using Spark",dest='local',default=False)
    parser.add_argument("--blankTile",type=str,help="Hexadecimal of color without hashtag",required=True)

    return parser.parse_args()


class GlobalMercator(object):
    """
    TMS Global Mercator Profile
    ---------------------------

    Functions necessary for generation of tiles in Spherical Mercator projection,
    EPSG:900913 (EPSG:gOOglE, Google Maps Global Mercator), EPSG:3785, OSGEO:41001.

    Such tiles are compatible with Google Maps, Microsoft Virtual Earth, Yahoo Maps,
    UK Ordnance Survey OpenSpace API, ...
    and you can overlay them on top of base maps of those web mapping applications.

    Pixel and tile coordinates are in TMS notation (origin [0,0] in bottom-left).

    What coordinate conversions do we need for TMS Global Mercator tiles::

         LatLon      <->       Meters      <->     Pixels    <->       Tile

     WGS84 coordinates   Spherical Mercator  Pixels in pyramid  Tiles in pyramid
         lat/lon            XY in metres     XY pixels Z zoom      XYZ from TMS
        EPSG:4326           EPSG:900913
         .----.              ---------               --                TMS
        /      \     <->     |       |     <->     /----/    <->      Google
        \      /             |       |           /--------/          QuadTree
         -----               ---------         /------------/
       KML, public         WebMapService         Web Clients      TileMapService

    What is the coordinate extent of Earth in EPSG:900913?

      [-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244]
      Constant 20037508.342789244 comes from the circumference of the Earth in meters,
      which is 40 thousand kilometers, the coordinate origin is in the middle of extent.
      In fact you can calculate the constant as: 2 * math.pi * 6378137 / 2.0
      $ echo 180 85 | gdaltransform -s_srs EPSG:4326 -t_srs EPSG:900913
      Polar areas with abs(latitude) bigger then 85.05112878 are clipped off.

    What are zoom level constants (pixels/meter) for pyramid with EPSG:900913?

      whole region is on top of pyramid (zoom=0) covered by 256x256 pixels tile,
      every lower zoom level resolution is always divided by two
      initialResolution = 20037508.342789244 * 2 / 256 = 156543.03392804062

    What is the difference between TMS and Google Maps/QuadTree tile name convention?

      The tile raster itself is the same (equal extent, projection, pixel size),
      there is just different identification of the same raster tile.
      Tiles in TMS are counted from [0,0] in the bottom-left corner, id is XYZ.
      Google placed the origin [0,0] to the top-left corner, reference is XYZ.
      Microsoft is referencing tiles by a QuadTree name, defined on the website:
      http://msdn2.microsoft.com/en-us/library/bb259689.aspx

    The lat/lon coordinates are using WGS84 datum, yeh?

      Yes, all lat/lon we are mentioning should use WGS84 Geodetic Datum.
      Well, the web clients like Google Maps are projecting those coordinates by
      Spherical Mercator, so in fact lat/lon coordinates on sphere are treated as if
      the were on the WGS84 ellipsoid.

      From MSDN documentation:
      To simplify the calculations, we use the spherical form of projection, not
      the ellipsoidal form. Since the projection is used only for map display,
      and not for displaying numeric coordinates, we don't need the extra precision
      of an ellipsoidal projection. The spherical projection causes approximately
      0.33 percent scale distortion in the Y direction, which is not visually noticable.

    How do I create a raster in EPSG:900913 and convert coordinates with PROJ.4?

      You can use standard GIS tools like gdalwarp, cs2cs or gdaltransform.
      All of the tools supports -t_srs 'epsg:900913'.

      For other GIS programs check the exact definition of the projection:
      More info at http://spatialreference.org/ref/user/google-projection/
      The same projection is degined as EPSG:3785. WKT definition is in the official
      EPSG database.

      Proj4 Text:
        +proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0
        +k=1.0 +units=m +nadgrids=@null +no_defs

      Human readable WKT format of EPGS:900913:
         PROJCS["Google Maps Global Mercator",
             GEOGCS["WGS 84",
                 DATUM["WGS_1984",
                     SPHEROID["WGS 84",6378137,298.2572235630016,
                         AUTHORITY["EPSG","7030"]],
                     AUTHORITY["EPSG","6326"]],
                 PRIMEM["Greenwich",0],
                 UNIT["degree",0.0174532925199433],
                 AUTHORITY["EPSG","4326"]],
             PROJECTION["Mercator_1SP"],
             PARAMETER["central_meridian",0],
             PARAMETER["scale_factor",1],
             PARAMETER["false_easting",0],
             PARAMETER["false_northing",0],
             UNIT["metre",1,
                 AUTHORITY["EPSG","9001"]]]
    """

    def __init__(self, tileSize=256):
        "Initialize the TMS Global Mercator pyramid"
        self.tileSize = tileSize
        self.initialResolution = 2 * math.pi * 6378137 / self.tileSize
        # 156543.03392804062 for tileSize 256 pixels
        self.originShift = 2 * math.pi * 6378137 / 2.0
        # 20037508.342789244

    def LatLonToMeters(self, lat, lon ):
        "Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:900913"

        mx = lon * self.originShift / 180.0
        my = math.log( math.tan((90 + lat) * math.pi / 360.0 )) / (math.pi / 180.0)

        my = my * self.originShift / 180.0
        return mx, my

    def MetersToLatLon(self, mx, my ):
        "Converts XY point from Spherical Mercator EPSG:900913 to lat/lon in WGS84 Datum"

        lon = (mx / self.originShift) * 180.0
        lat = (my / self.originShift) * 180.0

        lat = 180 / math.pi * (2 * math.atan( math.exp( lat * math.pi / 180.0)) - math.pi / 2.0)
        return lat, lon

    def PixelsToMeters(self, px, py, zoom):
        "Converts pixel coordinates in given zoom level of pyramid to EPSG:900913"

        res = self.Resolution( zoom )
        mx = px * res - self.originShift
        my = py * res - self.originShift
        return mx, my

    def MetersToPixels(self, mx, my, zoom):
        "Converts EPSG:900913 to pyramid pixel coordinates in given zoom level"

        res = self.Resolution( zoom )
        px = (mx + self.originShift) / res
        py = (my + self.originShift) / res
        return px, py

    def PixelsToTile(self, px, py):
        "Returns a tile covering region in given pixel coordinates"

        tx = int( math.ceil( px / float(self.tileSize) ) - 1 )
        ty = int( math.ceil( py / float(self.tileSize) ) - 1 )
        return tx, ty

    def PixelsToRaster(self, px, py, zoom):
        "Move the origin of pixel coordinates to top-left corner"

        mapSize = self.tileSize << zoom
        return px, mapSize - py

    def MetersToTile(self, mx, my, zoom):
        "Returns tile for given mercator coordinates"

        px, py = self.MetersToPixels( mx, my, zoom)
        return self.PixelsToTile( px, py)

    def TileBounds(self, tx, ty, zoom):
        "Returns bounds of the given tile in EPSG:900913 coordinates"

        minx, miny = self.PixelsToMeters( tx*self.tileSize, ty*self.tileSize, zoom )
        maxx, maxy = self.PixelsToMeters( (tx+1)*self.tileSize, (ty+1)*self.tileSize, zoom )
        return ( minx, miny, maxx, maxy )

    def TileLatLonBounds(self, tx, ty, zoom ):
        "Returns bounds of the given tile in latutude/longitude using WGS84 datum"

        bounds = self.TileBounds( tx, ty, zoom)
        minLat, minLon = self.MetersToLatLon(bounds[0], bounds[1])
        maxLat, maxLon = self.MetersToLatLon(bounds[2], bounds[3])

        return ( minLat, minLon, maxLat, maxLon )

    def Resolution(self, zoom ):
        "Resolution (meters/pixel) for given zoom level (measured at Equator)"

        # return (2 * math.pi * 6378137) / (self.tileSize * 2**zoom)
        return self.initialResolution / (2**zoom)

    def ZoomForPixelSize(self, pixelSize ):
        "Maximal scaledown zoom of the pyramid closest to the pixelSize."

        for i in range(30):
            if pixelSize > self.Resolution(i):
                return i-1 if i!=0 else 0 # We don't want to scale up

    def GoogleTile(self, tx, ty, zoom):
        "Converts TMS tile coordinates to Google Tile coordinates"

        # coordinate origin is moved from bottom-left to top-left corner of the extent
        return tx, (2**zoom - 1) - ty

    def QuadTree(self, tx, ty, zoom ):
        "Converts TMS tile coordinates to Microsoft QuadTree"

        quadKey = ""
        ty = (2**zoom - 1) - ty
        for i in range(zoom, 0, -1):
            digit = 0
            mask = 1 << (i-1)
            if (tx & mask) != 0:
                digit += 1
            if (ty & mask) != 0:
                digit += 2
            quadKey += str(digit)

        return quadKey

# get the cache levels for a specified index
# e.g. for index 2048 this is 000/002/048
def cacheLevels(index):

    level1 = str(index // (1000 * 1000)).zfill(3)
    level2 = str(index // 1000).zfill(3)
    level3 = str(index % 1000).zfill(3)
    return level1, level2, level3


def buildFromPrefix(bucket, collection, date, zoomFrom, toTileIndex, x, y, blankTile):

    fromXLevels = cacheLevels((toTileIndex[0] * 2) + x)
    fromYLevels = cacheLevels((toTileIndex[1] * 2) + y)

    blank = os.path.join(collection, "g", "blanks", blankTile + ".png")

    fromPrefix = os.path.join(collection, "g", date, zoomFrom,
                              fromXLevels[0], fromXLevels[1], fromXLevels[2],
                              fromYLevels[0], fromYLevels[1], fromYLevels[2] + ".png")

    print(fromPrefix)

    s3 = boto3.resource('s3', endpoint_url="https://cf2.cloudferro.com:8080", region_name="RegionOne")
    bucket = s3.Bucket(bucket)
    bucket_files = [x.key for x in bucket.objects.filter(Prefix=fromPrefix)]

    # if file doesn't exist or file is a symlink to a blank file, consider it as blank
    # latter can be removed when pyramis is built in a temporary cache root
    if not bucket_files:
        fromPrefix = blank

    return fromPrefix


def generateUpperTile(toTileIndex, endpoint_url, region, bucket, collection, zoomFrom, topZoom, date, compress, blankTile):
    from PIL import Image, PngImagePlugin

    setupLogger()

    logger = logging.getLogger("py4j")

    zoomTo = str(zoomFrom - 1).zfill(2) # e.g. 03
    zoomFrom = str(zoomFrom).zfill(2) # e.g. 04
    topZoom = str(topZoom).zfill(2)

    toXLevels = cacheLevels(toTileIndex[0])
    toYLevels = cacheLevels(toTileIndex[1])

    blank = os.path.join(collection, "g", "blanks", blankTile + ".png")

    fromX0Y0Path = buildFromPrefix(bucket, collection, date, zoomFrom, toTileIndex, 0, 0, blankTile)
    fromX0Y1Path = buildFromPrefix(bucket, collection, date, zoomFrom, toTileIndex, 0, 1, blankTile)
    fromX1Y0Path = buildFromPrefix(bucket, collection, date, zoomFrom, toTileIndex, 1, 0, blankTile)
    fromX1Y1Path = buildFromPrefix(bucket, collection, date, zoomFrom, toTileIndex, 1, 1, blankTile)

    if(fromX0Y0Path == blank and fromX1Y0Path == blank
            and fromX0Y1Path == blank and fromX1Y1Path == blank):
        logger.info("Skipping tile %s, level %s because found no non-empty parent tiles found", toTileIndex, zoomTo)

    else:
        logger.info("Processing tile %s, level %s", toTileIndex, zoomTo)
        toKey = os.path.join(collection, "g", date, zoomTo,
                             toXLevels[0], toXLevels[1], toXLevels[2],
                             toYLevels[0], toYLevels[1], toYLevels[2] + ".png")

        if blankTile == "00000000":
            toImage = Image.new("RGBA", (512,512))
        else:
            toImage = Image.new("RGB", (512,512))

        def get_file(prefix):
            return next(iter(s3_bucket(endpoint_url, region, bucket).objects.filter(Prefix=prefix))).get()["Body"]

        if fromX0Y0Path != blank:
            fromX0Y0Image = Image.open(get_file(fromX0Y0Path))
            toImage.paste(fromX0Y0Image, box=(0, 256))
        if fromX1Y0Path != blank:
            fromX1Y0Image = Image.open(get_file(fromX1Y0Path))
            toImage.paste(fromX1Y0Image, box=(256, 256))
        if fromX0Y1Path != blank:
            fromX0Y1Image = Image.open(get_file(fromX0Y1Path))
            toImage.paste(fromX0Y1Image, box=(0, 0))
        if fromX1Y1Path != blank:
            fromX1Y1Image = Image.open(get_file(fromX1Y1Path))
            toImage.paste(fromX1Y1Image, box=(256, 0))

        toImage = toImage.resize((256, 256), resample=Image.BICUBIC)

        # if this is the last level we need to generate, we can already convert to PNG8
        if compress and zoomTo == topZoom:
            toImage.convert('P', palette=Image.ADAPTIVE, colors=256)

        put_s3_object(toImage, bucket, toKey)
        toImage.close()

        def convertRGBToPNG8(tile: Image, key: str):
            info = PngImagePlugin.PngInfo()
            info.add_text("generated_by", "generatePyramid")

            tile = tile.convert('P', palette=Image.ADAPTIVE, colors=256)
            put_s3_object(tile, bucket, key)

        # now also convert the "from" tiles to PNG8
        if compress:
            if fromX0Y0Path != blank:
                convertRGBToPNG8(fromX0Y0Image, fromX0Y0Path)
                fromX0Y0Image.close()
            if fromX1Y0Path != blank:
                convertRGBToPNG8(fromX1Y0Image, fromX1Y0Path)
                fromX1Y0Image.close()
            if fromX0Y1Path != blank:
                convertRGBToPNG8(fromX0Y1Image, fromX0Y1Path)
                fromX0Y1Image.close()
            if fromX1Y1Path != blank:
                convertRGBToPNG8(fromX1Y1Image, fromX1Y1Path)
                fromX1Y1Image.close()


def main():

    args = parseArguments()

    print("Generating pyramid: ")
    print(args)

    sc = None
    if not args.local:
        from pyspark import SparkContext, SparkConf
        sc = SparkContext.getOrCreate(SparkConf().setAppName("generatePyramid:{}:{}".format(args.collection, args.date)))

    try:
        endpoint_url = args.endpoint_url
        region = args.region
        bucket = args.bucket
        prefix = os.path.join(args.collection, "g", args.date)

        minX, minY, maxX, maxY = toMinMax(bucket, prefix, args.bottomZoom)

        for zoomFrom in reversed(range(args.topZoom + 1, args.bottomZoom + 1)):
            # from zoom level zoomFrom, generate upper level (zoom - 1)
            minX, minY, maxX, maxY = int(minX/2), int(minY/2), int(maxX/2)+1, int(maxY/2)+1

            print("Zoom level: " + str(zoomFrom))
            print("BBox: X:" + str (minX) + "-" + str(maxX) + " Y:" + str(minY) + "-" + str(maxY))
            if args.local:
                for x in range(minX, maxX):
                    for y in range(minY,maxY):
                        generateUpperTile((x,y), endpoint_url=endpoint_url, region=region, bucket=args.bucket, collection=args.collection, zoomFrom=zoomFrom,
                                          topZoom=args.topZoom, date=args.date, compress=args.compress, blankTile=args.blankTile)
            else:
                xRDD = sc.parallelize(range(minX, maxX))
                yRDD = sc.parallelize(range(minY, maxY))
                xRDD.cartesian(yRDD).repartition(max(4,int((maxX-minX)*(maxY-minY)/20000))).foreach(
                    partial(generateUpperTile, endpoint_url=endpoint_url, region=region, bucket=args.bucket, collection=args.collection, zoomFrom=zoomFrom,
                            topZoom=args.topZoom, date=args.date, compress=args.compress, blankTile=args.blankTile))
    finally:
        if sc != None:
            sc.stop()


def s3(endpoint_url, region):
    return boto3.resource('s3', endpoint_url=endpoint_url, region_name=region)


def s3_bucket(endpoint_url, region, bucket):
    return s3(endpoint_url, region).Bucket(bucket)


def put_s3_object(image, bucket, key):
    from io import BytesIO
    buffer = BytesIO()
    image.save(buffer, format='PNG', quality=95)
    buffer.seek(0)

    s3().Object(bucket, key).put(Body=buffer)


def toMinMax(bucket, prefix, bottomZoom):
    import re

    bucket_files = [x.key for x in s3_bucket(bucket).objects.filter(Prefix=os.path.join(prefix, str(bottomZoom).zfill(2)))]

    def dirToXY(dir):
        matches = re.search(r".*(.{3}\/.{3}\/.{3})\/(.{3}\/.{3}\/.{3})\.png", dir)
        x = int(matches.group(1).replace("/", ""))
        y = int(matches.group(2).replace("/", ""))
        return x, y

    coordinates = list(map(dirToXY, bucket_files))

    print(coordinates)

    if len(coordinates) == 0:
        return 0, 0, 0, 0
    else:
        minX, maxX, minY, maxY = 2 ** bottomZoom - 1, 0, 2 ** bottomZoom - 1, 0
        for xy in coordinates:
            x, y = xy
            minX = min(x, minX)
            minY = min(y, minY)
            maxX = max(x, maxX)
            maxY = max(y, maxY)

        return minX, minY, maxX, maxY


if __name__ == "__main__":
    main()