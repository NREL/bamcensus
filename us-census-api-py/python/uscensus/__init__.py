# # begin maturin header
# from .us_census_api_py import *


# __doc__ = us_census_api_py.__doc__
# if hasattr(us_census_api_py, "__all__"):
#     __all__ = us_census_api_py.__all__
# # end maturin header


import us_census_api_py as api
import pandas as pd
import geopandas as gpd
from shapely import wkt


def run_wac_tiger(year: int, **kwds):
    data = api.wac_tiger(year, **kwds)
    df = pd.DataFrame.from_dict(data, orient="index")
    df["geometry"] = df.geometry.apply(wkt.loads)
    df = gpd.GeoDataFrame(df, crs="EPSG:4326")
    return df


__all__ = "run_wac_tiger"
