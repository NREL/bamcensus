import bamsoda_app_py as api
import pandas as pd
import geopandas as gpd
from shapely import wkt


def run_wac_tiger(year: int, **kwds):
    data = api.run_wac_tiger_python(year, **kwds)
    df = pd.DataFrame.from_dict(data, orient="index")
    df["geometry"] = df.geometry.apply(wkt.loads)
    df = gpd.GeoDataFrame(df, crs="EPSG:4326")
    return df


def run_acs_tiger(year: int, **kwds):
    print(api.__all__)
    data = api.run_acs_tiger_python(year, **kwds)
    df = pd.DataFrame.from_dict(data, orient="index")
    df["geometry"] = df.geometry.apply(wkt.loads)
    df = gpd.GeoDataFrame(df, crs="EPSG:4326")
    return df


__all__ = ("run_wac_tiger", "run_acs_tiger")
