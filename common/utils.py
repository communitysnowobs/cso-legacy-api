from functools import wraps
import time
from io import BytesIO
import threading
import pandas as pd
import urllib.request
import geopandas as gpd
from datetime import datetime
import tarfile
import polyline
from shapely.geometry import Point, Polygon, shape
from osgeo import gdal, gdal_array, osr
import re
import numpy as np

def empty_cso_dataframe():
    return pd.DataFrame(columns = [
        'author_name',
        'id',
        'timestamp',
        'lat',
        'long',
        'snow_depth',
        'source',
        'elevation'
    ])

def date_to_timestamp(date):
    """Converts datetime object to unix timestamp.

    Keyword arguments:
    date -- Datetime object to convert
    """
    if date is None:
        return date
    return int(time.mktime(date.timetuple())) * 1000

def timestamp_to_date(timestamp):
    """Converts unix timestamp to datettime object.

    Keyword arguments:
    timestamp -- Timestamp to convert
    """
    if timestamp is None:
        return timestamp
    return datetime.fromtimestamp(timestamp / 1000)

def decoded_polygon(str):
    if str:
        try:
            split = str.split('|')
            decoded = [tuple(map(int, coord.split(','))) for coord in split]
            decoded = [x[::-1] for x in decoded]
            polygon = Polygon(decoded)
            return polygon
        except:
            pass
        try:
            decoded = polyline.decode(str)
            decoded = [x[::-1] for x in decoded]
            polygon = Polygon(decoded)
            return polygon
        except:
            return None

def most_recent_hour():
    now = time.time() * 1000
    dt = timestamp_to_date(now).replace(minute=0, second=0)
    return date_to_timestamp(dt)

def gdal_metadata(source):
    """Get metadata from GDAL dataset.

    Keyword arguments:
    source -- GDAL dataset to retrieve metadata for
    """
    ndv = source.GetRasterBand(1).GetNoDataValue()
    width = source.RasterXSize
    height = source.RasterYSize
    transform = source.GetGeoTransform()
    projection = osr.SpatialReference()
    projection.ImportFromWkt(source.GetProjectionRef())
    dtype = gdal.GetDataTypeName(source.GetRasterBand(1).DataType)

    return ndv, width, height, transform, projection, dtype

def url_to_io(url):
    """Get raw bytes from url.

    Keyword arguments:
    url -- URL to fetch data from
    """
    stream = urllib.request.urlopen(url)
    bytes = BytesIO()
    while True:
        next = stream.read(16384)
        if not next:
            break

        bytes.write(next)

    stream.close()
    bytes.seek(0)
    return bytes

def url_to_tar(url):
    """Get tar object from url.

    Keyword arguments:
    url -- URL of SNODAS data for specific date
    """
    io = url_to_io(url)
    tar = tarfile.open(fileobj = io, mode = 'r')
    return tar

def save_ds(ds, path, driver):
    """Save GDAL dataset using arbitrary driver.

    Keyword arguments:
    ds -- GDAl dataset
    path -- Location where file will be saved
    driver -- Driver to use
    """

    band = ds.GetRasterBand(1)
    bytes = band.ReadAsArray()
    driver = gdal.GetDriverByName(driver)
    ndv, width, height, transform, projection, dtype = gdal_metadata(ds)
    bytes[np.isnan(bytes)] = ndv
    out_ds = driver.Create(path, width, height, 1, gdal.GDT_Int16)
    out_ds.SetGeoTransform(transform)
    out_ds.SetProjection(projection.ExportToWkt())

    out_ds.GetRasterBand(1).WriteArray(bytes)
    out_ds.GetRasterBand(1).SetNoDataValue(ndv)

def save_tiff(ds, path):
    """Save GDAL dataset as GeoTIFF file.

    Keyword arguments:
    ds -- GDAl dataset
    path -- Location where file will be saved
    """
    save_ds(ds, path, 'GTiff')

def save_netcdf(ds, path):
    """Save GDAL dataset as NetCDF file.

    Keyword arguments:
    ds -- GDAl dataset
    path -- Location where file will be saved
    """
    save_ds(ds, path, 'netCDF')

def error_message(str):
    return { 'message' : str }

def data_message(str):
    return { 'data' : str}
