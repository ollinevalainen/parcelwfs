"""
Interface to get field parcel data from Finnish Food Authority (Ruokavirasto)
Author: Olli Nevalainen, Finnish Meteorological Institute
"""

import json
import logging
import requests
import pandas as pd
from pydantic import BaseModel
import geopandas as gpd
from urllib.error import HTTPError

try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


from shapely.geometry import Point
from pyproj import Transformer, CRS
from owslib.wfs import WebFeatureService

logger = logging.getLogger(__name__)

PARCEL_SEP = "-"

# FINNISH_FOOD_AUTHORITY_WFS = "https://inspire.ruokavirasto-awsa.com/geoserver/wfs"
# AGRICULTURAL_PARCEL_LAYER_BASENAME = (
#     "inspire:LandUse.ExistingLandUse.GSAAAgriculturalParcel."
# )


# class WFSLayer(StrEnum):
#     AGRICULTURAL_PARCEL = "inspire:LandUse.ExistingLandUse.GSAAAgriculturalParcel"
#     REFERENCE_PARCEL = "inspire:LC.LandCoverSurfaces.LPIS"


class ParcelType(StrEnum):
    GSAA = "gsaa"
    LPIS = "lpis"


class WFSLayers(BaseModel):
    gsaa: str
    lpis: str


class GSAAPropertyMapping(BaseModel):
    id: str
    year: int
    lpis_parcel_id: str
    species_code: str
    species_description: str
    area: float
    gsaa_parcel_name: str  # TODO Naming ok?


class LPISPropertyMapping(BaseModel):
    id: str
    year: int
    lpis_parcel_id: str
    # organic_farming: int
    # sloped_area: float
    # groundwater_area: float
    # natura_area: float
    area: float


# class AgriParcelProperty(StrEnum):
#     ID = "id"
#     YEAR = "VUOSI"
#     REFERENCE_PARCEL_ID = "PERUSLOHKOTUNNUS"
#     SPECIES_CODE = "KASVIKOODI"
#     SPECIES_DESCRIPTION = "KASVIKOODI_SELITE_FI"
#     AREA = "PINTA_ALA"
#     PARCEL_NUMBER = "LOHKONUMERO"


# class ReferenceParcelProperty(StrEnum):
#     ID = "id"
#     YEAR = "VUOSI"
#     REFERENCE_PARCEL_ID = "PERUSLOHKOTUNNUS"
#     ORGANIC_FARMING = "LUOMUVILJELY"
#     SLOPED_AREA = "KALTEVA_ALA"
#     GROUNDWATER_AREA = "POHJAVESI_ALA"
#     NATURA_AREA = "NATURA_ALA"
#     AREA = "PINTA_ALA"


class ParcelWFS(BaseModel):
    country_id: str
    endpoint: str
    layers: WFSLayers
    gsaa_properties: GSAAPropertyMapping
    lpis_properties: LPISPropertyMapping
    wfs_version: str = "2.0.0"  # Not sure if even works with versions < 2.0.0

    @classmethod
    def from_json(cls, file_path: str) -> "ParcelWFS":
        with open(file_path, "r", encoding="utf-8") as fp:
            json_data = json.load(fp)
        return ParcelWFS.model_validate(json_data)

    def get_available_layers(self) -> list:
        wfs = WebFeatureService(url=self.endpoint, version=self.wfs_version)
        return list(wfs.contents)

    def get_available_parcel_layers(
        self, parcel_type: ParcelType = ParcelType.GSAA
    ) -> list | None:
        all_layers = self.get_available_layers()
        if parcel_type == ParcelType.GSAA:
            field_parcel_layers = [
                layer for layer in all_layers if self.layers.gsaa in layer
            ]
        elif parcel_type == ParcelType.LPIS:
            field_parcel_layers = [
                layer for layer in all_layers if self.layers.lpis in layer
            ]
        else:
            err_msg = f"Unknown parcel type {parcel_type}."
            logger.error(err_msg)
            return None

        return field_parcel_layers

    def get_available_parcel_years(
        self, parcel_type: ParcelType = ParcelType.GSAA
    ) -> list | None:
        field_parcel_layers = self.get_available_parcel_layers(parcel_type)
        if not field_parcel_layers:
            return None
        else:
            return [int(layer.split(".")[-1]) for layer in field_parcel_layers]

    def query(
        self, query_filter: str, year: int, parcel_type: ParcelType
    ) -> gpd.GeoDataFrame:
        years_available = self.get_available_parcel_years(parcel_type)
        if year not in years_available:
            raise ValueError(
                f"""Field parcel layer not available for year {year}. Currently
                            available years: {years_available}"""
            )
        if parcel_type == ParcelType.GSAA:
            layer_name = f"{self.layers.gsaa}.{year}"
        elif parcel_type == ParcelType.LPIS:
            layer_name = f"{self.layers.lpis}.{year}"
        else:
            err_msg = f"Unknown parcel type {parcel_type}."
            logger.error(err_msg)
            return None

        params = dict(
            service="WFS",
            version=self.wfs_version,
            request="GetFeature",
            typeName=layer_name,
            cql_filter=query_filter,
            outputFormat="json",
        )

        # Parse the URL with parameters
        wfs_request_url = (
            requests.Request("GET", self.endpoint, params=params).prepare().url
        )

        # Read data from URL
        try:
            gdf = gpd.read_file(wfs_request_url)
            if gdf.empty:
                logger.info(f"No field parcel with given query parameters: {params}.")
                return None
        except HTTPError as err:
            err_msg = "Error when querying WFS with URL. Possibly invalid parcel id."
            logger.error(err_msg)
            raise Exception(err_msg) from err
        return gdf

    def get_gsaa_parcels_by_lpis_parcel_id(
        self, lpis_parcel_id: str, year: int, output_crs: CRS | None = None
    ) -> gpd.GeoDataFrame | None:

        # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
        query_filter = f"{self.gsaa_properties.lpis_parcel_id}='{lpis_parcel_id}'"
        # Read data from URL
        gdf = self.query(query_filter, year, self.layers.gsaa)
        if gdf is None or gdf.empty:
            print(
                f"No field parcel for year {year} with given query parameters:"
                f"{query_filter}."
            )
            return None
        if output_crs is not None:
            gdf = gdf.to_crs(crs=output_crs)
        return gdf

    def get_gsaa_parcel_by_id(
        self, gsaa_parcel_id: str, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        """

        Parameters
        ----------
        gsaa_parcel_id : str
            GSAA parcel ID of form '{LPIS_PARCEL_ID}-{GSAA_PARCEL_NAME}'.
        year : int
            Get parcel for this year.
        """
        gsaa_parcel_id_split = gsaa_parcel_id.split(PARCEL_SEP)
        lpis_parcel_id = gsaa_parcel_id_split[0]
        gsaa_parcel_name = gsaa_parcel_id_split[1]
        # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
        query_filter = (
            f"{self.gsaa_properties.lpis_parcel_id}='{lpis_parcel_id}' "
            f"AND {self.gsaa_properties.parcel_name}='{gsaa_parcel_name}'"
        )
        # Read data from URL
        gdf = self.query(query_filter, year, self.layers.gsaa)
        if gdf is None:
            logger.info(
                f"No field parcel for year {year} with given query parameters:"
                f"{query_filter}."
            )
            return None
        else:
            if output_crs is not None:
                gdf = gdf.to_crs(crs=output_crs)
            return gdf.iloc[0]

    def get_parcel_by_point(
        self,
        point_in_wfs_crs: Point,
        year: int,
        parcel_type: ParcelType,
        output_crs: CRS | None = None,
    ) -> pd.Series:
        x = point_in_wfs_crs.x
        y = point_in_wfs_crs.y
        spatial_filter = f"Intersects(geom,POINT ({x} {y}))"
        # Read data from URL
        gdf = self.query(spatial_filter, year, parcel_type)
        if gdf.empty:
            logger.info(
                f"No field parcel with given query parameters: {spatial_filter}."
            )
            return None
        else:
            if output_crs is not None:
                gdf = gdf.to_crs(crs=output_crs)
            return gdf.iloc[0]

    @staticmethod
    def point_in_source_crs_from_lat_lon(
        lat: float, lon: float, source_crs: CRS
    ) -> Point:
        transformer_to_source_crs = Transformer.from_crs("epsg:4326", source_crs)
        x, y = transformer_to_source_crs.transform(lat, lon)
        point = Point(x, y)
        return point

    def get_layer_crs(self, parcel_type: ParcelType, year: int):
        years_available = self.get_available_parcel_years(parcel_type)
        if year not in years_available:
            raise ValueError(
                f"""Field parcel layer not available for year {year}. Currently
                            available years: {years_available}"""
            )
        if parcel_type == ParcelType.GSAA:
            layer_name = f"{self.layers.gsaa}.{year}"
        elif parcel_type == ParcelType.LPIS:
            layer_name = f"{self.layers.lpis}.{year}"
        else:
            err_msg = f"Unknown parcel type {parcel_type}."
            logger.error(err_msg)
            return None

        wfs = WebFeatureService(url=self.endpoint, version=self.wfs_version)
        layer_crs = wfs.contents[layer_name].crsOptions[0]
        crs = CRS.from_user_input(layer_crs)
        return crs

    def get_gsaa_parcel_by_lat_lon(
        self, lat: float, lon: float, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        source_crs = self.get_layer_crs(ParcelType.GSAA, year)
        point = self.point_in_source_crs_from_lat_lon(lat, lon, source_crs=source_crs)

        parcel = self.get_parcel_by_point(point, year, ParcelType.GSAA, output_crs)
        return parcel

    def get_lpis_parcel_by_id(
        self, lpis_parcel_id: str, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
        query_filter = f"{self.lpis_properties.lpis_parcel_id}='{lpis_parcel_id}'"
        # Read data from URL
        gdf = self.query(query_filter, year, ParcelType.LPIS)
        if gdf is None or gdf.empty:
            logger.info(
                f"No field parcel for year {year} with given query parameters:"
                f"{query_filter}."
            )
            return None
        else:
            if output_crs is not None:
                gdf = gdf.to_crs(crs=output_crs)
            return gdf.iloc[0]

    def get_lpis_parcel_by_lat_lon(
        self, lat: float, lon: float, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        source_crs = self.get_layer_crs(ParcelType.LPIS, year)
        point = self.point_in_source_crs_from_lat_lon(lat, lon, source_crs=source_crs)
        parcel = self.get_parcel_by_point(point, year, ParcelType.LPIS, output_crs)
        return parcel

    def handle_year_input(
        self, year: int | list[int] | None, parcel_type: ParcelType
    ) -> list[int]:
        if year is None:
            years = self.get_available_parcel_years(parcel_type)
        elif isinstance(year, int):
            years = [year]
        elif isinstance(year, list):
            years = year
        return years

    def get_gsaa_parcel_species_by_lat_lon(
        self, lat: float, lon: float, year: int | list[int] | None = None
    ) -> dict:
        years = self.handle_year_input(year, ParcelType.GSAA)

        species_information = {}
        for year in years:
            parcel = self.get_gsaa_parcel_by_lat_lon(lat, lon, year)
            if parcel is not None:
                species_information[year] = self.species_information_from_gsaa_parcel(
                    parcel
                )

        return species_information

    def get_gsaa_parcel_species_by_gsaa_parcel_id(
        self, gsaa_parcel_id: str, year: int
    ) -> dict | None:
        """

        Parameters
        ----------
        gsaa_parcel_id : str
            GSAA parcel ID of form '{LPIS_PARCEL_ID}-{GSAA_PARCEL_NAME}'.

        """

        parcel = self.get_gsaa_parcel_by_id(gsaa_parcel_id, year)
        if parcel is not None:
            return self.species_information_from_gsaa_parcel(parcel)
        else:
            logger.error(
                f"No field parcel with given query parameters: {gsaa_parcel_id}."
                f" Please check the parcel ID format."
            )
            return None

    def species_information_from_gsaa_parcel(self, gsaa_parcel: pd.Series) -> dict:
        gsaa_parcel_id = (
            f"{gsaa_parcel[self.gsaa_properties.year]}{PARCEL_SEP}"
            f"{gsaa_parcel[self.gsaa_properties.lpis_parcel_id]}{PARCEL_SEP}"
            f"{gsaa_parcel[self.gsaa_properties.gsaa_parcel_name]}"
        )
        species_information = {
            "parcel_id": gsaa_parcel_id,
            "lpis_parcel_id": gsaa_parcel[self.gsaa_properties.lpis_parcel_id],
            "species_code": gsaa_parcel[self.gsaa_properties.species_code],
            "species_description": gsaa_parcel[
                self.gsaa_properties.species_description
            ],
        }
        return species_information

    def species_information_for_lpis_parcel_id(
        self, lpis_parcel_id: str, year: int
    ) -> dict | None:
        gsaa_parcels = self.get_gsaa_parcels_by_lpis_parcel_id(lpis_parcel_id, year)
        if gsaa_parcels is None:
            logger.error(
                f"No field parcel with given query parameters: {lpis_parcel_id}."
            )
            return None

        max_area_species_info = (
            gsaa_parcels.groupby(
                [
                    self.gsaa_properties.species_code,
                    self.gsaa_properties.species_description,
                ]
            )[[self.gsaa_properties.area]]
            .sum()
            .idxmax()
        )
        parcel_id = f"{year}{PARCEL_SEP}{lpis_parcel_id}"
        species_information = {
            "parcel_id": parcel_id,
            "lpis_parcel_id": lpis_parcel_id,
            "species_code_FI": max_area_species_info.iloc[0][0],
            "species_description_FI": max_area_species_info.iloc[0][1],
        }
        return species_information
