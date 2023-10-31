import os, sys
import pandas
import shapely
import geopandas

def open_geopandas_csv(file_path: str) -> geopandas.GeoDataFrame:
    df = pandas.read_csv(file_path)
    return geopandas.GeoDataFrame(df.drop('geometry', axis=1), df['geometry'].apply(shapely.wkt.loads))

def get_neighbourhoods() -> geopandas.GeoDataFrame:
    curr_dir = os.path.dirname(__file__)
    main_dir = os.path.dirname(curr_dir)
    data_dir = os.path.join(main_dir, 'constext-data')
    file_dir = os.path.join(data_dir, 'Neighbourhood.csv')
    return open_geopandas_csv(file_dir)
