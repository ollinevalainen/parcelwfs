"""
Interface to get field parcel data from Finnish Food Authority (Ruokavirasto)
Author: Olli Nevalainen, Finnish Meteorological Institute
"""

import requests
import pandas as pd
import geopandas as gpd
from enum import Enum
from typing import Optional, Union, List
from urllib.error import HTTPError
from shapely.geometry import Point
from pyproj import Transformer
from owslib.wfs import WebFeatureService

FINNISH_FOOD_AUTHORITY_WFS = "https://inspire.ruokavirasto-awsa.com/geoserver/wfs"
AGRICULTURAL_PARCEL_LAYER_BASENAME = (
    "inspire:LandUse.ExistingLandUse.GSAAAgriculturalParcel."
)


class AgriParcelProperty(str, Enum):
    ID = "id"
    YEAR = "VUOSI"
    REFERENCE_PARCEL_ID = "PERUSLOHKOTUNNUS"
    SPECIES_CODE = "KASVIKOODI"
    SPECIES_DESCRIPTION = "KASVIKOODI_SELITE_FI"
    AREA = "PINTA_ALA"
    PARCEL_NUMBER = "LOHKONUMERO"


def get_available_layers() -> list:
    wfs = WebFeatureService(url=FINNISH_FOOD_AUTHORITY_WFS, version="2.0.0")
    return list(wfs.contents)


def get_available_parcel_layers() -> list:
    all_layers = get_available_layers()
    field_parcel_layers = [
        layer for layer in all_layers if AGRICULTURAL_PARCEL_LAYER_BASENAME in layer
    ]
    return field_parcel_layers


def get_available_parcel_years() -> list:
    field_parcel_layers = get_available_parcel_layers()
    years_available = [int(layer.split(".")[-1]) for layer in field_parcel_layers]
    return years_available


def query(query_filter: str, year: int) -> gpd.GeoDataFrame:
    years_available = get_available_parcel_years()
    if year not in years_available:
        raise ValueError(
            f"""Field parcel layer not available for year {year}. Currently
                         available years: {years_available}"""
        )

    layer_name = f"{AGRICULTURAL_PARCEL_LAYER_BASENAME}{year}"
    params = dict(
        service="WFS",
        version="2.0.0",
        request="GetFeature",
        typeName=layer_name,
        cql_filter=query_filter,
        outputFormat="json",
    )

    # Parse the URL with parameters
    wfs_request_url = (
        requests.Request("GET", FINNISH_FOOD_AUTHORITY_WFS, params=params).prepare().url
    )

    # Read data from URL
    try:
        gdf = gpd.read_file(wfs_request_url)
        if gdf.empty:
            print(f"No field parcel with given query parameters: {params}.")
            return None
    except HTTPError as err:
        raise Exception("Possibly invalid parcel id.") from err
    return gdf


def get_parcels_by_reference_parcel_id(
    reference_parcel_id: str, year: int
) -> gpd.GeoDataFrame:
    # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
    query_filter = (
        f"{AgriParcelProperty.REFERENCE_PARCEL_ID.value}='{reference_parcel_id}'"
    )
    # Read data from URL
    gdf = query(query_filter, year)
    return gdf


def get_parcel_by_parcel_id(agri_parcel_id: str, year: int) -> pd.Series:
    """

    Parameters
    ----------
    agri_parcel_id : str
        Agricultural parcel ID of form '{REFERENCE_PARCEL_ID}-{PARCEL_NUMBER}'.
    year : int
        Agricultural parcel for this year.
    """
    reference_parcel_id = agri_parcel_id.split("-")[0]
    parcel_number = agri_parcel_id.split("-")[1]
    # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
    query_filter = (
        f"{AgriParcelProperty.REFERENCE_PARCEL_ID.value}='{reference_parcel_id}' "
        f"AND {AgriParcelProperty.PARCEL_NUMBER.value}='{parcel_number}'"
    )
    # Read data from URL
    gdf = query(query_filter, year)
    if gdf is None:
        print(
            f"No field parcel for year {year} with given query parameters:"
            f"{query_filter}."
        )
        return None
    else:
        return gdf.iloc[0]


def get_parcel_by_point3067(point_in_epsg3067: Point, year: int) -> pd.Series:
    x = point_in_epsg3067.x
    y = point_in_epsg3067.y
    spatial_filter = f"Intersects(geom,POINT ({x} {y}))"
    # Read data from URL
    gdf = query(spatial_filter, year)
    if gdf.empty:
        print(f"No field parcel with given query parameters: {spatial_filter}.")
        return None
    else:
        return gdf.iloc[0]


def get_parcel_by_lat_lon(lat: float, lon: float, year: int) -> pd.Series:
    transformer_to_3067 = Transformer.from_crs("epsg:4326", "epsg:3067")
    x, y = transformer_to_3067.transform(lat, lon)
    point = Point(x, y)
    parcel = get_parcel_by_point3067(point, year)
    return parcel


def _handle_year_input(year: Optional[Union[int, List[int]]]) -> List[int]:
    if year is None:
        years = get_available_parcel_years()
    elif isinstance(year, int):
        years = [year]
    elif isinstance(year, list):
        years = year
    return years


def get_parcel_species_by_lat_lon(
    lat: float, lon: float, year: Optional[Union[int, List[int]]] = None
) -> dict:
    years = _handle_year_input(year)

    species_information = {}
    for year in years:
        parcel = get_parcel_by_lat_lon(lat, lon, year)
        if parcel is not None:
            species_information[year] = species_information_from_parcel(parcel)

    return species_information


def get_parcel_species_by_parcel_id(
    agri_parcel_id: str, year: Optional[Union[int, List[int]]] = None
) -> dict:
    """

    Parameters
    ----------
    agri_parcel_id : str
        Agricultural parcel ID of form '{REFERENCE_PARCEL_ID}-{PARCEL_NUMBER}'.
    year : int
        Agricultural parcel for this year.
    """

    years = _handle_year_input(year)

    species_information = {}
    for year in years:
        parcel = get_parcel_by_parcel_id(agri_parcel_id, year)
        if parcel is not None:
            species_information[year] = species_information_from_parcel(parcel)

    return species_information


def species_information_from_parcel(agri_parcel: pd.Series) -> dict:
    agri_parcel_id = (
        f"{agri_parcel[AgriParcelProperty.REFERENCE_PARCEL_ID]}-"
        f"{agri_parcel[AgriParcelProperty.PARCEL_NUMBER]}"
    )
    species_information = {
        "parcel_id": agri_parcel_id,
        "reference_parcel_id": agri_parcel[AgriParcelProperty.REFERENCE_PARCEL_ID],
        "species_code_FI": agri_parcel[AgriParcelProperty.SPECIES_CODE],
        "species_description_FI": agri_parcel[AgriParcelProperty.SPECIES_DESCRIPTION],
    }
    return species_information
