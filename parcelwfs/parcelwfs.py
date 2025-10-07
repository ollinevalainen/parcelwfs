"""
Interface to get field parcel data from national GSAA and LPIS WFS services.

This module provides a unified interface for querying agricultural field parcel data
from national Geospatial Aid Applications (GSAA) and Land Parcel Identification System
(LPIS) Web Feature Services (WFS). It supports querying parcels by ID, coordinates,
and spatial filters, with automatic handling of country-specific WFS configurations.

The module uses YAML configuration files to define country-specific WFS endpoints,
layer names, and property mappings, allowing easy extension to new countries.

Classes
-------
ParcelType : StrEnum
    Enumeration for parcel types (GSAA or LPIS).
WFSLayers : BaseModel
    Configuration model for WFS layer names.
Endpoints : BaseModel
    Configuration model for WFS endpoint URLs.
GSAAPropertyMapping : BaseModel
    Mapping of GSAA parcel properties to WFS attributes.
LPISPropertyMapping : BaseModel
    Mapping of LPIS parcel properties to WFS attributes.
ParcelWFS : BaseModel
    Main class for interacting with parcel WFS services.

Examples
--------
Load a country's WFS configuration and query parcels:

>>> from parcelwfs import ParcelWFS
>>> wfs = ParcelWFS.get_by_id("FI")  # Load Finnish WFS configuration
>>> parcel = wfs.get_lpis_parcel_by_id("12345", year=2023)
>>> gsaa_parcels = wfs.get_gsaa_parcels_by_lpis_parcel_id("12345", year=2023)

Query parcels by coordinates:

>>> parcel = wfs.get_lpis_parcel_by_lat_lon(60.1699, 24.9384, year=2023)
>>> species_info = wfs.get_gsaa_parcel_species_by_lat_lon(60.1699, 24.9384, year=2023)

Author: Olli Nevalainen, Finnish Meteorological Institute
"""

import logging
import requests
import pandas as pd
from pydantic import BaseModel, field_validator
import geopandas as gpd
import numpy as np
from urllib.error import HTTPError
from pathlib import Path

try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


import yaml
from shapely.geometry import Point
from pyproj import Transformer, CRS
from owslib.wfs import WebFeatureService

logger = logging.getLogger(__name__)

PARCEL_SEP = "/"


class ParcelType(StrEnum):
    """
    Enumeration of parcel types.

    Attributes
    ----------
    GSAA : str
        Geospatial Aid Applications parcels.
    LPIS : str
        Land Parcel Identification System parcels.
    """

    GSAA = "gsaa"
    LPIS = "lpis"


class WFSLayers(BaseModel):
    """
    WFS layer name configuration for parcel types.

    Attributes
    ----------
    gsaa : str
        Base layer name for GSAA parcels (year suffix is appended).
    lpis : str
        Base layer name for LPIS parcels (year suffix is appended).
    """

    gsaa: str
    lpis: str


class Endpoints(BaseModel):
    """
    WFS endpoint URL configuration.

    Attributes
    ----------
    gsaa : str
        URL endpoint for GSAA WFS service.
    lpis : str
        URL endpoint for LPIS WFS service.
    """

    gsaa: str
    lpis: str


class GSAAPropertyMapping(BaseModel):
    """
    Mapping of GSAA parcel properties to WFS attributes.

    Defines the property names used in the WFS service for GSAA parcels.

    Attributes
    ----------
    id : str
        Property name for parcel ID.
    year : str or None
        Property name for year. Defaults to "year" if None.
    lpis_parcel_id : str
        Property name for the parent LPIS parcel ID.
    species_code : str
        Property name for crop species code.
    species_description : str
        Property name for crop species description.
    area : str
        Property name for parcel area.
    gsaa_parcel_name : str
        Property name for GSAA parcel name/number.
    geometry : str
        Property name for parcel geometry.
    """

    id: str
    year: str | None
    lpis_parcel_id: str
    species_code: str
    species_description: str
    area: str
    gsaa_parcel_name: str  # TODO Naming ok?
    geometry: str

    @field_validator("year", mode="before")
    @classmethod
    def set_year_default(cls, v):
        """Set default value for year if None is provided."""
        if v is None:
            return "year"  # or any default value
        return v


class LPISPropertyMapping(BaseModel):
    """
    Mapping of LPIS parcel properties to WFS attributes.

    Defines the property names used in the WFS service for LPIS parcels.

    Attributes
    ----------
    id : str
        Property name for parcel ID.
    year : str or None
        Property name for year. Defaults to "year" if None.
    lpis_parcel_id : str
        Property name for LPIS parcel ID.
    area : str
        Property name for parcel area.
    geometry : str
        Property name for parcel geometry.
    """

    id: str
    year: str | None
    lpis_parcel_id: str
    area: str
    geometry: str


class ParcelWFS(BaseModel):
    """
    Main class for interacting with parcel WFS services.

    This class provides methods to query agricultural field parcel data from
    WFS services, supporting both GSAA and LPIS parcel types. It handles
    country-specific configurations through YAML files.

    Attributes
    ----------
    id : str
        Country identifier (e.g., "FI", "DK").
    endpoints : Endpoints
        WFS endpoint URLs for GSAA and LPIS services.
    layers : WFSLayers
        Base layer names for GSAA and LPIS parcels.
    gsaa_properties : GSAAPropertyMapping
        Property name mappings for GSAA parcels.
    lpis_properties : LPISPropertyMapping
        Property name mappings for LPIS parcels.
    wfs_version : str, default="2.0.0"
        WFS protocol version to use.

    Examples
    --------
    >>> wfs = ParcelWFS.get_by_id("FI")
    >>> parcel = wfs.get_lpis_parcel_by_id("12345", year=2023)
    >>> species = wfs.get_gsaa_parcel_species_by_lat_lon(60.1699, 24.9384, year=2023)
    """

    id: str
    endpoints: Endpoints
    layers: WFSLayers
    gsaa_properties: GSAAPropertyMapping
    lpis_properties: LPISPropertyMapping
    wfs_version: str = "2.0.0"  # Not sure if even works with versions < 2.0.0

    @classmethod
    def from_yaml(cls, file_path: str) -> "ParcelWFS":
        """
        Load ParcelWFS configuration from a YAML file.

        Parameters
        ----------
        file_path : str
            Path to the YAML configuration file.

        Returns
        -------
        ParcelWFS
            Configured ParcelWFS instance.
        """
        with open(file_path, "r", encoding="utf-8") as fp:
            yaml_data = yaml.safe_load(fp)
        return ParcelWFS.model_validate(yaml_data)

    @classmethod
    def get_by_id(cls, parcelwfs_id: str) -> "ParcelWFS":
        """
        Load ParcelWFS configuration by country ID.

        Parameters
        ----------
        parcelwfs_id : str
            Country code identifier (e.g., "FI" for Finland, "DK" for Denmark).

        Returns
        -------
        ParcelWFS
            Configured ParcelWFS instance for the specified country.
        """
        parcel_wfs_definition_file = Path(__file__).parent / f"{parcelwfs_id}.yaml"
        return cls.from_yaml(parcel_wfs_definition_file)

    def get_available_layers(self, parcel_type: ParcelType) -> list:
        """
        Get all available WFS layers for a parcel type.

        Parameters
        ----------
        parcel_type : ParcelType
            Type of parcel (GSAA or LPIS).

        Returns
        -------
        list
            List of available layer names in the WFS service.
        """
        wfs = WebFeatureService(
            url=getattr(self.endpoints, parcel_type.value), version=self.wfs_version
        )
        return list(wfs.contents)

    def get_available_parcel_layers(
        self, parcel_type: ParcelType = ParcelType.GSAA
    ) -> list | None:
        """
        Get available parcel layers filtered by parcel type.

        Parameters
        ----------
        parcel_type : ParcelType, default=ParcelType.GSAA
            Type of parcel (GSAA or LPIS).

        Returns
        -------
        list or None
            List of layer names matching the parcel type, or None if no layers found.
        """
        all_layers = self.get_available_layers(parcel_type=parcel_type)

        field_parcel_layers = [
            layer
            for layer in all_layers
            if getattr(self.layers, parcel_type.value) in layer
        ]

        return field_parcel_layers

    def get_available_parcel_years(
        self, parcel_type: ParcelType = ParcelType.GSAA
    ) -> list | None:
        """
        Get available years for parcel data.

        Parameters
        ----------
        parcel_type : ParcelType, default=ParcelType.GSAA
            Type of parcel (GSAA or LPIS).

        Returns
        -------
        list or None
            List of available years as integers, or None if no layers found.
        """
        field_parcel_layers = self.get_available_parcel_layers(parcel_type)
        if not field_parcel_layers:
            return None
        else:
            return [
                int(layer.split(getattr(self.layers, parcel_type.value))[-1])
                for layer in field_parcel_layers
            ]

    def handle_output(
        self,
        gdf: gpd.GeoDataFrame | None,
        year: int,
        to_series: bool,
        output_crs: CRS | None,
    ) -> gpd.GeoDataFrame | pd.Series | None:
        """
        Process and format query output.

        Handles coordinate transformation, adds year column if missing, and
        converts to Series if requested.

        Parameters
        ----------
        gdf : gpd.GeoDataFrame or None
            Query result GeoDataFrame.
        year : int
            Year to add to results if not present.
        to_series : bool
            If True, return first row as Series instead of GeoDataFrame.
        output_crs : CRS or None
            Target coordinate reference system for reprojection.

        Returns
        -------
        gpd.GeoDataFrame, pd.Series, or None
            Processed results, None if input is empty.
        """
        if gdf is None or gdf.empty:
            return None
        if output_crs is not None:
            gdf = gdf.to_crs(crs=output_crs)

        # Add year column if not present
        if (
            self.gsaa_properties.year not in gdf.columns
            or self.lpis_properties.year not in gdf.columns
        ):
            gdf["year"] = year
        if to_series:
            return gdf.iloc[0]
        else:
            return gdf

    def query(
        self, query_filter: str, year: int, parcel_type: ParcelType
    ) -> gpd.GeoDataFrame:
        """
        Execute a WFS query with the specified filter.

        Parameters
        ----------
        query_filter : str
            CQL filter expression for the WFS query.
        year : int
            Year to query.
        parcel_type : ParcelType
            Type of parcel (GSAA or LPIS).

        Returns
        -------
        gpd.GeoDataFrame
            Query results as a GeoDataFrame.

        Raises
        ------
        ValueError
            If the requested year is not available.
        Exception
            If the WFS query fails.
        """
        years_available = self.get_available_parcel_years(parcel_type)
        if year not in years_available:
            raise ValueError(
                f"""Field parcel layer not available for year {year}. Currently
                            available years: {years_available}"""
            )
        layer_name = f"{getattr(self.layers, parcel_type.value)}{year}"

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
            requests.Request(
                "GET", getattr(self.endpoints, parcel_type.value), params=params
            )
            .prepare()
            .url
        )

        # Read data from URL
        try:
            gdf = gpd.read_file(wfs_request_url)

        except UnicodeDecodeError:
            logger.debug("UnicodeDecodeError caught, trying with different encoding.")
            try:
                gdf = gpd.read_file(wfs_request_url, encoding="latin-1")
            except Exception as err:
                logger.error(f"Error reading WFS response with latin-1 encoding: {err}")
                return None
        except HTTPError as err:
            err_msg = "Error when querying WFS with URL. Possibly invalid parcel id."
            logger.error(err_msg)
            raise Exception(err_msg) from err
        finally:
            if gdf.empty:
                logger.info(f"No field parcel with given query parameters: {params}.")
                return None
        return gdf

    def get_gsaa_parcels_by_lpis_parcel_id(
        self, lpis_parcel_id: str, year: int, output_crs: CRS | None = None
    ) -> gpd.GeoDataFrame | None:
        """
        Get all GSAA parcels associated with an LPIS parcel ID.

        Parameters
        ----------
        lpis_parcel_id : str
            LPIS parcel identifier.
        year : int
            Year to query.
        output_crs : CRS, optional
            Target coordinate reference system for output geometries.

        Returns
        -------
        gpd.GeoDataFrame or None
            GeoDataFrame of all GSAA parcels within the LPIS parcel, or None if not found.
        """
        # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
        query_filter = f"{self.gsaa_properties.lpis_parcel_id}='{lpis_parcel_id}'"
        # Read data from URL
        gdf = self.query(query_filter, year, ParcelType.GSAA)
        return self.handle_output(gdf, year, to_series=False, output_crs=output_crs)

    def get_gsaa_parcel_by_id(
        self, gsaa_parcel_id: str, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        """
        Get a specific GSAA parcel by its full ID.

        Parameters
        ----------
        gsaa_parcel_id : str
            GSAA parcel ID of form '{LPIS_PARCEL_ID}/{GSAA_PARCEL_NAME}'.
        year : int
            Get parcel for this year.
        output_crs : CRS, optional
            Target coordinate reference system for output geometry.

        Returns
        -------
        pd.Series
            Series containing the GSAA parcel data and geometry.
        """
        gsaa_parcel_id_split = gsaa_parcel_id.split(PARCEL_SEP)
        lpis_parcel_id = gsaa_parcel_id_split[0]
        gsaa_parcel_name = gsaa_parcel_id_split[1]
        # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
        query_filter = (
            f"{self.gsaa_properties.lpis_parcel_id}='{lpis_parcel_id}' "
            f"AND {self.gsaa_properties.gsaa_parcel_name}='{gsaa_parcel_name}'"
        )
        # Read data from URL
        gdf = self.query(query_filter, year, ParcelType.GSAA)
        return self.handle_output(gdf, year, to_series=True, output_crs=output_crs)

    def get_parcel_by_point(
        self,
        point_in_wfs_crs: Point,
        year: int,
        parcel_type: ParcelType,
        output_crs: CRS | None = None,
    ) -> pd.Series:
        """
        Get a parcel that intersects with a given point.

        Parameters
        ----------
        point_in_wfs_crs : Point
            Point geometry in the WFS layer's coordinate system.
        year : int
            Year to query.
        parcel_type : ParcelType
            Type of parcel (GSAA or LPIS).
        output_crs : CRS, optional
            Target coordinate reference system for output geometry.

        Returns
        -------
        pd.Series
            Series containing the parcel data and geometry.
        """
        x = point_in_wfs_crs.x
        y = point_in_wfs_crs.y
        geom_property = (
            self.gsaa_properties.geometry
            if parcel_type == ParcelType.GSAA
            else self.lpis_properties.geometry
        )
        spatial_filter = f"Intersects({geom_property},POINT ({x} {y}))"
        # Read data from URL
        gdf = self.query(spatial_filter, year, parcel_type)
        return self.handle_output(gdf, year, to_series=True, output_crs=output_crs)

    @staticmethod
    def point_in_source_crs_from_lat_lon(
        lat: float, lon: float, source_crs: CRS
    ) -> Point:
        """
        Transform a lat/lon point to a point in the source CRS.

        Parameters
        ----------
        lat : float
            Latitude in WGS84 (EPSG:4326).
        lon : float
            Longitude in WGS84 (EPSG:4326).
        source_crs : CRS
            Target coordinate reference system.

        Returns
        -------
        Point
            Point geometry in the target CRS.
        """
        transformer_to_source_crs = Transformer.from_crs("epsg:4326", source_crs)
        x, y = transformer_to_source_crs.transform(lat, lon)
        # Check if coordinates are not NaN or infinite
        if np.isnan(x) or np.isnan(y) or np.isinf(x) or np.isinf(y):
            err_msg = f"Invalid coordinates after transformation: {x}, {y}"
            logger.error(err_msg)
            raise ValueError(err_msg)
        point = Point(x, y)
        return point

    def get_layer_crs(self, parcel_type: ParcelType, year: int):
        """
        Get the coordinate reference system for a WFS layer.

        Parameters
        ----------
        parcel_type : ParcelType
            Type of parcel (GSAA or LPIS).
        year : int
            Year of the layer.

        Returns
        -------
        CRS
            Coordinate reference system of the layer.

        Raises
        ------
        ValueError
            If the layer for the given year is not available.
        """
        years_available = self.get_available_parcel_years(parcel_type)
        if year not in years_available:
            raise ValueError(
                f"""Field parcel layer not available for year {year}. Currently
                            available years: {years_available}"""
            )
        layer_name = f"{getattr(self.layers, parcel_type.value)}{year}"

        wfs = WebFeatureService(
            url=getattr(self.endpoints, parcel_type.value), version=self.wfs_version
        )
        layer_crs = wfs.contents[layer_name].crsOptions[0]
        crs = CRS.from_user_input(layer_crs.id)
        return crs

    def get_gsaa_parcel_by_lat_lon(
        self, lat: float, lon: float, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        """
        Get a GSAA parcel by latitude and longitude coordinates.

        Parameters
        ----------
        lat : float
            Latitude in WGS84 (EPSG:4326).
        lon : float
            Longitude in WGS84 (EPSG:4326).
        year : int
            Year to query.
        output_crs : CRS, optional
            Target coordinate reference system for output geometry.

        Returns
        -------
        pd.Series
            Series containing the GSAA parcel data and geometry at the given location.
        """
        source_crs = self.get_layer_crs(ParcelType.GSAA, year)
        point = self.point_in_source_crs_from_lat_lon(lat, lon, source_crs=source_crs)

        parcel = self.get_parcel_by_point(point, year, ParcelType.GSAA, output_crs)
        return parcel

    def get_lpis_parcel_by_id(
        self, lpis_parcel_id: str, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        """
        Get an LPIS parcel by its ID.

        Parameters
        ----------
        lpis_parcel_id : str
            LPIS parcel identifier.
        year : int
            Year to query.
        output_crs : CRS, optional
            Target coordinate reference system for output geometry.

        Returns
        -------
        pd.Series
            Series containing the LPIS parcel data and geometry.
        """
        # Need to single quote the parcel id, otherwise won't work with parcel IDs starting with 0
        query_filter = f"{self.lpis_properties.lpis_parcel_id}='{lpis_parcel_id}'"
        # Read data from URL
        gdf = self.query(query_filter, year, ParcelType.LPIS)
        return self.handle_output(gdf, year, to_series=True, output_crs=output_crs)

    def get_lpis_parcel_by_lat_lon(
        self, lat: float, lon: float, year: int, output_crs: CRS | None = None
    ) -> pd.Series:
        """
        Get an LPIS parcel by latitude and longitude coordinates.

        Parameters
        ----------
        lat : float
            Latitude in WGS84 (EPSG:4326).
        lon : float
            Longitude in WGS84 (EPSG:4326).
        year : int
            Year to query.
        output_crs : CRS, optional
            Target coordinate reference system for output geometry.

        Returns
        -------
        pd.Series
            Series containing the LPIS parcel data and geometry at the given location.
        """
        source_crs = self.get_layer_crs(ParcelType.LPIS, year)
        point = self.point_in_source_crs_from_lat_lon(lat, lon, source_crs=source_crs)
        parcel = self.get_parcel_by_point(point, year, ParcelType.LPIS, output_crs)
        return parcel

    def handle_year_input(
        self, year: int | list[int] | None, parcel_type: ParcelType
    ) -> list[int]:
        """
        Convert year input to a list of years.

        Parameters
        ----------
        year : int, list[int], or None
            Year(s) to process. If None, returns all available years.
        parcel_type : ParcelType
            Type of parcel to get available years for.

        Returns
        -------
        list[int]
            List of years to query.
        """
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
        """
        Get species information for GSAA parcels at a given location.

        Parameters
        ----------
        lat : float
            Latitude in WGS84 (EPSG:4326).
        lon : float
            Longitude in WGS84 (EPSG:4326).
        year : int, list[int], or None, optional
            Year(s) to query. If None, queries all available years.

        Returns
        -------
        dict
            Dictionary mapping years to species information dictionaries.
        """
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
        Get species information for a specific GSAA parcel.

        Parameters
        ----------
        gsaa_parcel_id : str
            GSAA parcel ID of form '{LPIS_PARCEL_ID}/{GSAA_PARCEL_NAME}'.
        year : int
            Year to query.

        Returns
        -------
        dict or None
            Dictionary containing species information, or None if parcel not found.
        """

        parcel = self.get_gsaa_parcel_by_id(gsaa_parcel_id, year)

        if self.gsaa_properties.year not in parcel:
            parcel[self.gsaa_properties.year] = year

        if parcel is not None:
            return self.species_information_from_gsaa_parcel(parcel)
        else:
            logger.error(
                f"No field parcel with given query parameters: {gsaa_parcel_id}."
                f" Please check the parcel ID format."
            )
            return None

    def species_information_from_gsaa_parcel(self, gsaa_parcel: pd.Series) -> dict:
        """
        Extract species information from a GSAA parcel Series.

        Parameters
        ----------
        gsaa_parcel : pd.Series
            Series containing GSAA parcel data.

        Returns
        -------
        dict
            Dictionary with parcel_id, lpis_parcel_id, species_code,
            species_description, area, and geometry.
        """
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
        """
        Get aggregated species information for all GSAA parcels in an LPIS parcel.

        Returns the species with the largest total area within the LPIS parcel.

        Parameters
        ----------
        lpis_parcel_id : str
            LPIS parcel identifier.
        year : int
            Year to query.

        Returns
        -------
        dict or None
            Dictionary with lpis_parcel_id, species_code, species_description,
            and total area for the dominant species, or None if no parcels found.
        """
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
