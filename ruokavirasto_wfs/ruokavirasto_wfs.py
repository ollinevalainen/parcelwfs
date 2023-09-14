    """
    Interface to get field parcel data from Finnish Food Authority (Ruokavirasto)
    Author: Olli Nevalainen, Finnish Meteorological Institute
    """

import geopandas as gpd
from requests import Request
from typing import List, Optional
from shapely.geometry import Point
from pyproj import Transformer
from owslib.wfs import WebFeatureService

FINNISH_FOOD_AUTHORITY_WFS = "https://inspire.ruokavirasto-awsa.com/geoserver/wfs"
FIELD_PARCEL_LAYER_BASENAME = "inspire:LandUse.ExistingLandUse.GSAAAgriculturalParcel."
FIELD_PARCEL_ID_PROPERTY = "PERUSLOHKOTUNNUS"
FIELD_PARCEL_SPECIES_ID_FI = "KASVIKOODI"
FIELD_PARCEL_SPECIES_DESCRIPTION_FI = "KASVIKOODI_SELITE_FI"

def get_available_layers():
    wfs = WebFeatureService(url=FINNISH_FOOD_AUTHORITY_WFS, version="2.0.0")
    return list(wfs.contents)

def get_available_field_parcel_layers():
    all_layers = get_available_layers()
    field_parcel_layers = [layer for layer in all_layers if FIELD_PARCEL_LAYER_BASENAME in layer]
    return field_parcel_layers


def get_available_field_parcel_years():
    field_parcel_layers = get_available_field_parcel_layers()
    years_available = [int(layer.split(".")[-1]) for layer in field_parcel_layers]
    return years_available


def _get_field_parcel(query_filter:str, year: int) -> gpd.GeoDataFrame:

    layer_name = f"{FIELD_PARCEL_LAYER_BASENAME}{year}"
    params = dict(service='WFS', version="2.0.0", request='GetFeature',
        typeName=layer_name, cql_filter=query_filter, outputFormat='json')

    # Parse the URL with parameters
    wfs_request_url = Request("GET",FINNISH_FOOD_AUTHORITY_WFS, params=params).prepare().url

    # Read data from URL
    gdf = gpd.read_file(wfs_request_url)
    return gdf


def get_field_parcel_by_parcel_id(parcel_id: str, year: int) -> gpd.GeoDataFrame:
    query_filter = f"{FIELD_PARCEL_ID_PROPERTY}={parcel_id}"
    # Read data from URL
    gdf = _get_field_parcel(query_filter, year)
    return gdf


def get_field_parcel_by_point3067(point_in_epsg3067: Point, year: int) -> gpd.GeoDataFrame:
    x = point_in_epsg3067.x
    y = point_in_epsg3067.y
    spatial_filter=f"Intersects(geom,POINT ({x} {y}))"
    # Read data from URL
    gdf = _get_field_parcel(spatial_filter, year)
    return gdf


def get_field_parcel_by_lat_lon(lat: float, lon: float, year: int):
    transformer_to_3067 = Transformer.from_crs("epsg:4326", "epsg:3067")
    x, y = transformer_to_3067.transform(lat, lon)
    point = Point(x, y)
    gdf = get_field_parcel_by_point3067(point, year)
    return gdf


def get_field_parcels_by_parcel_id(parcel_ids: List[str], year: int) -> gpd.GeoDataFrame:
    gdf = gpd.GeoDataFrame()
    for parcel_id in parcel_ids:
        gdf_tmp = get_field_parcel_by_parcel_id(parcel_id, year)
        gdf = gpd.concat([gdf, gdf_tmp])
    return gdf


def get_field_parcel_species(lat:float , lon:float, year: Optional[int]=None):

    if year:
        years = [year]
    else:
        years = get_available_field_parcel_years()

    species_information = {}
    for year in years:
        gdf = get_field_parcel_by_lat_lon(lat, lon, year)            
        species_information[year] = {"species_code_FI": gdf[FIELD_PARCEL_SPECIES_ID_FI][0],
                                    "species_description_FI": gdf[FIELD_PARCEL_SPECIES_DESCRIPTION_FI][0]}

    return species_information