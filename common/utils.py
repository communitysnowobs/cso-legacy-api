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



def unsafe(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            return None
    return wrapper

def batch(size=16):
    def decorator(func):
        @wraps(func)
        def wrapper(l):
            assert isinstance(l, list), "data has to be in list form."
            results = []
            for i in range(0, len(l), size):
                result = func(l[i:i + size])
                results.extend(result)
            return results
        return wrapper
    return decorator

def threaded(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

def cache(ttl=60, max_size=128):

    hash_separator = object()
    cache = {}
    queue = []

    def insert(key, val):
        # Make sure cache does not exceed max_size
        while len(cache) >= max_size or len(queue) >= max_size:
            remove()
        cache[key] = (time.time(), val)
        queue.append(key)

    def update(key, val):
        # Move key to end of queue and update value
        if key in queue:
            index = queue.index(key)
            del queue[index]
        queue.append(key)
        cache[key] = (time.time(), val)

    def remove():
        # Remove oldest key from cache
        key = queue[0]
        del queue[0]
        del cache[key]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get unique key based on arguments passed to function
            key = hash(args + (hash_separator,) + tuple(sorted(kwargs.items())))
            if key in cache:
                cached_at, val = cache.get(key)
                if time.time() - cached_at > ttl:
                    print('expired: ', args, kwargs)
                    new_val = func(*args, **kwargs)
                    update(key, new_val)
                    return val
                else:
                    print('cached: ', args, kwargs)
                    return val
            else:
                print('not cached: ', args, kwargs)
                val = func(*args, **kwargs)
                insert(key, val)
                return val

        return wrapper
    return decorator

def empty_cso_dataframe():
    return pd.DataFrame(columns = [
        'author_name',
        'id',
        'timestamp',
        'lat',
        'long',
        'type',
        'snow_depth',
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
