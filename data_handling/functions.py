import os, sys
import pandas
import shapely
import geopandas

CRS = "urn:ogc:def:crs:OGC:1.3:CRS84"



def get_data_path(file_name: str):
    """Returns path to store context data."""
    curr_dir = os.path.dirname(__file__)
    main_dir = os.path.dirname(curr_dir)
    data_dir = os.path.join(main_dir, 'context_data')
    return os.path.join(data_dir, file_name)



def store_context_data(gdf: geopandas.GeoDataFrame, file_name: str) -> None:
    """Stores a geopandas.GeoDataFrame to a .csv file."""
    file_path = get_data_path(file_name)

    pandas.DataFrame(gdf.to_crs(CRS)).to_csv(file_path)

    return



def open_geopandas_csv(file_path: str, **kwargs) -> geopandas.GeoDataFrame:
    ## Open the .csv using pandas
    df = pandas.read_csv(file_path, **kwargs)

    ## Transform the dataset into a geopandas dataset
    gdf = geopandas.GeoDataFrame(df.drop('geometry', axis=1), geometry=df['geometry'].apply(shapely.wkt.loads))

    ## Apply .crs to keep stuff somewhat standardized
    gdf.crs = CRS

    return gdf



def get_collisions() -> geopandas.GeoDataFrame:
    """Returns the dataframe containing collision data."""
    file_name = 'Traffic Collisions - Cleaned.csv'

    ## Path to the file
    curr_dir = os.path.dirname(__file__)
    main_dir = os.path.dirname(curr_dir)
    data_dir = os.path.join(main_dir, 'train_data')
    file_dir = os.path.join(data_dir, file_name)

    ## Return the geopandas dataframe
    return open_geopandas_csv(file_dir, index_col = 0)



def get_neighbourhoods() -> geopandas.GeoDataFrame:
    """Returns the dataframe containing neighbourhoods."""
    file_name = 'Neighbourhood.csv'

    ## Path to the file
    curr_dir = os.path.dirname(__file__)
    main_dir = os.path.dirname(curr_dir)
    data_dir = os.path.join(main_dir, 'context_data')
    file_dir = os.path.join(data_dir, file_name)

    ## Open the file from .csv format
    return open_geopandas_csv(file_dir)



def add_neighbourhood_column(gdf: geopandas.GeoDataFrame, nbhood_gdf: geopandas.GeoDataFrame = None, *, how = 'inner', predicate = 'within') -> geopandas.GeoDataFrame:
    """Adds a column named 'Neighbourhood', containing the neighbourhood IDs.

    PARAMS:
     - gdf:          The geopandas.GeoDataFrame with data to assign.
     - [nbhood_gdf]: The neighbourhood geopandas.GeoDataFrame.
     - [how]:        How to join the data (default 'inner' - only keep rows where a neighbourhood is found for gdf, to keep rows with no neighbourhood, use 'left')
     - [predicate]:  How to determine which rows to match.
       |->           Potential values for predicate include:
         |-> 'within'     -> Use this when gdf contains points in the geometry column
         |-> 'intersects' -> Use this when gdf contains lines/regions in the geometry column
    """

    ## Copy gdf input, in case input is a filtered GeoDataFrame object, instead of a GeoDataFrame instance
    gdf_copy = gdf.copy()

    if not nbhood_gdf:
        nbhood_gdf = get_neighbourhoods()

    ## Use the spacial join function, for all items within the neighbourhood
    joined = geopandas.sjoin(gdf_copy.to_crs(CRS), nbhood_gdf.to_crs(CRS), how = how, predicate = predicate,)


    ## When how == 'inner', some columns may be filtered out. Filter these columns in the output dataset.
    gdf_copy = gdf_copy.loc[joined.index]

    ## Neighbourhood SHOULD be stored as the index of nbhood_gdf
    gdf_copy['Neighbourhood'] = joined['index_right']

    return gdf_copy
