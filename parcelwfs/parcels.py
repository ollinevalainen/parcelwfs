"""
Parcel manipulation and merging utilities for agricultural field parcels.

This module provides classes and functions for working with agricultural field parcels,
including parcel geometry manipulation, merging small parcels based on area or width
criteria, and handling relationships between LPIS (Land Parcel Identification System)
and GSAA (Geospatial Aid Applications) parcels.

Classes
-------
MergingCriteria : StrEnum
    Enumeration of criteria for merging parcels.
Parcel : class
    Represents a field parcel with geometry and metadata.

Functions
---------
merge_geometries : Merge small parcels based on area and/or width thresholds.
merge_to_single_parts : Dissolve and explode geometries to single parts.
merge_by_shortest_boundary : Merge parcels by shortest resulting boundary.
merge_by_longest_intersection : Merge parcels by longest shared intersection.

Author: Olli Nevalainen, Finnish Meteorological Institute
"""

import logging
import shapely
import pandas as pd
import geopandas as gpd
import parcelwfs
from parcelwfs.parcelwfs import PARCEL_SEP

try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


from typing import Optional

logger = logging.getLogger(__name__)


MERGED_GEOM_PROPERTY = "merged_geometries"


class MergingCriteria(StrEnum):
    SHORTEST_BOUNDARY = "shortest_boundary"
    LONGEST_INTERSECTION = "longest_intersection"


class Parcel:
    """
    Represents an agricultural field parcel with geometry and metadata.

    A Parcel can represent either an LPIS parcel or one or more GSAA parcels.
    Parcel IDs are formatted as: {YEAR}/{LPIS_PARCEL_ID}/{GSAA_PARCEL_NAME(S)}.

    Parameters
    ----------
    parcel_id : str
        Full parcel identifier in format: year/lpis_id/gsaa_name(s).
    parcelwfs_id : str, optional
        Country code identifier to load the appropriate ParcelWFS configuration.
        Either parcelwfs_id or wfs must be provided.
    wfs : parcelwfs.ParcelWFS, optional
        Pre-configured ParcelWFS instance. Either parcelwfs_id or wfs must be provided.
    crs_int : int, default=4326
        EPSG code for the coordinate reference system to use for geometries.

    Attributes
    ----------
    parcel_id : str
        The full parcel identifier.
    wfs : parcelwfs.ParcelWFS
        The WFS service instance for querying parcel data.
    year : int
        Year extracted from the parcel_id.
    lpis_parcel_id : str
        LPIS parcel identifier.
    gsaa_parcel_names : list
        List of GSAA parcel names within the LPIS parcel.
    gsaa_parcel_ids : list
        List of full GSAA parcel identifiers.
    geometry : shapely.geometry or None
        The parcel geometry.
    crs_int : int
        EPSG code for the coordinate reference system.

    Examples
    --------
    >>> parcel = Parcel("2023/12345/1", parcelwfs_id="FI")
    >>> parcel.get_parcel_geometry()
    """

    def __init__(
        self,
        parcel_id: str,
        parcelwfs_id: str | None = None,
        wfs: parcelwfs.ParcelWFS | None = None,
        crs_int: int = 4326,
    ):
        self.parcel_id = parcel_id

        self.wfs = self.validate_parcelwfs_input(parcelwfs_id, wfs)

        self.year = int(parcel_id.split(PARCEL_SEP)[0])
        lpis_parcel, gsaa_parcel_names = self.extract_lpis_and_gsaa_from_parcel_id(
            parcel_id
        )
        self.lpis_parcel_id = lpis_parcel
        self.gsaa_parcel_names = gsaa_parcel_names
        self.gsaa_parcel_ids = [
            f"{self.lpis_parcel_id}{PARCEL_SEP}{parcel_number}"
            for parcel_number in self.gsaa_parcel_names
        ]
        self.geometry = None
        self.crs_int = crs_int

    @staticmethod
    def validate_parcelwfs_input(
        parcelwfs_id: str | None, wfs: parcelwfs.ParcelWFS | None
    ):
        """
        Validate and return a ParcelWFS instance from either an ID or direct instance.

        Parameters
        ----------
        parcelwfs_id : str or None
            Country code identifier to load ParcelWFS configuration.
        wfs : parcelwfs.ParcelWFS or None
            Pre-configured ParcelWFS instance.

        Returns
        -------
        parcelwfs.ParcelWFS
            The validated ParcelWFS instance.

        Raises
        ------
        ValueError
            If both parameters are None or both are provided.
        """
        if parcelwfs_id is None and wfs is None:
            err_msg = "Either parcelwfs_id or parcelwfs must be given"
            logger.error(err_msg)
            raise ValueError(err_msg)
        if parcelwfs_id is not None and wfs is not None:
            err_msg = "Only one of parcelwfs_id or parcelwfs must be given"
            logger.error(err_msg)
            raise ValueError(err_msg)
        if wfs is None:
            return parcelwfs.ParcelWFS.get_by_id(parcelwfs_id)
        else:
            return wfs

    @classmethod
    def get_parcels_from_wfs_gdf(
        cls, gdf: gpd.GeoDataFrame, wfs: parcelwfs.ParcelWFS
    ) -> list["Parcel"]:
        """
        Create a list of Parcel instances from a GeoDataFrame.

        Parameters
        ----------
        gdf : gpd.GeoDataFrame
            GeoDataFrame containing parcel data with 'parcel_id' column.
        wfs : parcelwfs.ParcelWFS
            ParcelWFS instance to associate with the parcels.

        Returns
        -------
        list[Parcel]
            List of Parcel instances created from the GeoDataFrame rows.
        """
        parcels = []
        for _, gsaa_parcel in gdf.iterrows():
            parcel = cls(gsaa_parcel.parcel_id, wfs=wfs)
            parcel.geometry = gsaa_parcel.geometry
            parcels.append(parcel)
        return parcels

    @classmethod
    def get_gsaa_parcels_by_lpis_parcel_id(
        cls,
        lpis_parcel_id: str,
        year: int,
        parcelwfs_id: str,
        wfs: parcelwfs.ParcelWFS | None = None,
        crs_int: int = 4326,
    ) -> list["Parcel"]:
        """
        Retrieve all GSAA parcels associated with an LPIS parcel ID.

        Parameters
        ----------
        lpis_parcel_id : str
            The LPIS parcel identifier.
        year : int
            The year to query.
        parcelwfs_id : str
            Country code identifier for ParcelWFS configuration.
        wfs : parcelwfs.ParcelWFS, optional
            Pre-configured ParcelWFS instance.
        crs_int : int, default=4326
            EPSG code for output coordinate reference system.

        Returns
        -------
        list[Parcel]
            List of GSAA Parcel instances for the given LPIS parcel.
        """
        wfs = cls.validate_parcelwfs_input(parcelwfs_id, wfs)

        gdf_gsaa_parcels = wfs.get_gsaa_parcels_by_lpis_parcel_id(
            lpis_parcel_id, year, output_crs=crs_int
        )
        gdf_gsaa_parcels = cls.add_parcel_id(wfs, gdf_gsaa_parcels)
        gsaa_parcels = cls.get_parcels_from_wfs_gdf(gdf_gsaa_parcels, wfs)
        return gsaa_parcels

    @classmethod
    def get_merged_gsaa_parcels_from_lpis_parcel_id(
        cls,
        lpis_parcel_id: str,
        year: int,
        parcelwfs_id: str | None = None,
        wfs: parcelwfs.ParcelWFS | None = None,
        min_area: float | None = None,
        min_width: float | None = None,
        crs_int: int = 4326,
    ) -> list["Parcel"]:
        """
        Retrieve GSAA parcels with small parcels merged based on area/width criteria.

        Small parcels (below thresholds) are merged with adjacent parcels to create
        more manageable parcel units.

        Parameters
        ----------
        lpis_parcel_id : str
            The LPIS parcel identifier.
        year : int
            The year to query.
        parcelwfs_id : str, optional
            Country code identifier for ParcelWFS configuration.
        wfs : parcelwfs.ParcelWFS, optional
            Pre-configured ParcelWFS instance.
        min_area : float, optional
            Minimum area threshold in hectares. Parcels below this are merged.
        min_width : float, optional
            Minimum width threshold in meters. Parcels below this are merged.
        crs_int : int, default=4326
            EPSG code for output coordinate reference system.

        Returns
        -------
        list[Parcel]
            List of Parcel instances with small parcels merged.
        """
        wfs = cls.validate_parcelwfs_input(parcelwfs_id, wfs)
        gdf_gsaa_parcels = wfs.get_gsaa_parcels_by_lpis_parcel_id(lpis_parcel_id, year)

        gdf_merged_gsaa = merge_geometries(
            gdf_gsaa_parcels, min_area=min_area, min_width=min_width
        ).to_crs(epsg=crs_int)

        gdf_merged_gsaa = cls.add_parcel_id(wfs, gdf_merged_gsaa, gdf_gsaa_parcels)
        merged_gsaa_parcels = cls.get_parcels_from_wfs_gdf(gdf_merged_gsaa, wfs)
        return merged_gsaa_parcels

    @staticmethod
    def extract_lpis_and_gsaa_from_parcel_id(parcel_id: str):
        """
        Extract LPIS and GSAA parcel components from a full parcel ID.

        Parameters
        ----------
        parcel_id : str
            Full parcel ID in format: year/lpis_id/gsaa_name(s).

        Returns
        -------
        tuple[str, list]
            Tuple of (lpis_parcel_id, list of gsaa_parcel_names).
        """
        parcels_str = parcel_id.split(PARCEL_SEP)
        lpis_parcel = parcels_str[1]
        gsaa_parcel_names = parcels_str[2:] if len(parcels_str) > 1 else []
        if isinstance(gsaa_parcel_names, str):
            gsaa_parcel_names = [gsaa_parcel_names]
        return lpis_parcel, gsaa_parcel_names

    def get_parcel_geometry(self):
        """
        Fetch and set the parcel geometry from the WFS service.

        For LPIS-only parcels, retrieves the LPIS geometry. For GSAA parcels,
        retrieves all GSAA parcel geometries and unions them into a single geometry.
        """
        if not self.gsaa_parcel_ids:
            self.geometry = self.wfs.get_lpis_parcel_by_id(
                self.lpis_parcel_id, self.year, output_crs=self.crs_int
            ).geometry
        else:
            geometries = []
            for gsaa_parcel_id in self.gsaa_parcel_ids:
                gsaa_parcel = self.wfs.get_gsaa_parcel_by_id(
                    gsaa_parcel_id, year=self.year, output_crs=self.crs_int
                )
                geometries.append(gsaa_parcel.geometry)
            self.geometry = shapely.union_all(geometries)

    @staticmethod
    def add_parcel_id(
        wfs: parcelwfs.ParcelWFS,
        gdf_merged: gpd.GeoDataFrame,
        gdf_original: Optional[gpd.GeoDataFrame] = None,
    ):
        """
        Add parcel_id column to a GeoDataFrame based on WFS property mappings.

        Creates parcel IDs in format: year/lpis_id/gsaa_name(s). For merged parcels,
        includes all original GSAA parcel names separated by '/'.

        Parameters
        ----------
        wfs : parcelwfs.ParcelWFS
            ParcelWFS instance with property mappings.
        gdf_merged : gpd.GeoDataFrame
            GeoDataFrame to add parcel_id column to.
        gdf_original : gpd.GeoDataFrame, optional
            Original GeoDataFrame before merging, required if gdf_merged contains
            merged geometries.

        Returns
        -------
        gpd.GeoDataFrame
            Input GeoDataFrame with added 'parcel_id' column.

        Raises
        ------
        ValueError
            If gdf_original is None when merged geometries are present.
        """
        parcel_ids = []
        gsaa_properties = wfs.gsaa_properties

        for _, gsaa_parcel in gdf_merged.iterrows():
            merged_parcel_idxs = getattr(gsaa_parcel, MERGED_GEOM_PROPERTY, None)
            lpis_parcel_id = getattr(gsaa_parcel, gsaa_properties.lpis_parcel_id)
            year = getattr(gsaa_parcel, gsaa_properties.year)

            if not merged_parcel_idxs:
                parcel_name = getattr(gsaa_parcel, gsaa_properties.gsaa_parcel_name)
                parcel_id = (
                    f"{year}{PARCEL_SEP}{lpis_parcel_id}{PARCEL_SEP}{parcel_name}"
                )
            else:
                # Check that the original gdf is given
                if gdf_original is None:
                    err_msg = (
                        "Original GeoDataFrame is needed to get the original parcel numbers"
                        "for merged parcels."
                    )
                    logger.error(err_msg)
                    raise ValueError(err_msg)
                parcel_names = list(
                    gdf_original.loc[
                        merged_parcel_idxs,
                        gsaa_properties.gsaa_parcel_name,
                    ].astype("string")
                )
                merged_ids = PARCEL_SEP.join(parcel_names)
                parcel_id = (
                    f"{year}{PARCEL_SEP}{lpis_parcel_id}{PARCEL_SEP}{merged_ids}"
                )
            parcel_ids.append(parcel_id)

        gdf_merged.loc[:, "parcel_id"] = parcel_ids
        return gdf_merged


def keep_equal_values(x):
    """
    Return the mode value if all values are equal, otherwise None.

    Used as an aggregation function when dissolving geometries.

    Parameters
    ----------
    x : array-like
        Values to aggregate.

    Returns
    -------
    value or None
        The common value if all values are equal, otherwise None.
    """
    return x.mode()[0] if x.nunique() == 1 else None


def merge_to_single_parts(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Dissolve and explode geometries to create single-part geometries.

    Dissolves all geometries together, then explodes any MultiPolygons into
    separate single Polygon features.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame with geometries to merge.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with dissolved and exploded single-part geometries.
    """
    gdf_union = gdf.dissolve(aggfunc=keep_equal_values)
    gdf_single_parts = gdf_union.explode(index_parts=False).reset_index(drop=True)
    return gdf_single_parts


def merge_by_shortest_boundary(
    candidate: pd.Series, targets: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge a candidate parcel with a target parcel that results in shortest boundary.

    Attempts to merge the candidate with each target. Returns updated targets
    where the candidate is merged with the target that produces the shortest
    perimeter after merging.

    Parameters
    ----------
    candidate : pd.Series
        Parcel to merge (must have 'geometry' attribute).
    targets : gpd.GeoDataFrame
        Candidate target parcels for merging.

    Returns
    -------
    gpd.GeoDataFrame or None
        Updated targets with candidate merged into the best match, or None if
        no valid merge is possible (all merges result in MultiPolygons).
    """
    new_targets = targets.copy(deep=True)
    merged_geometries = new_targets.geometry.apply(
        lambda x: shapely.union(candidate.geometry, x)
    )
    # Drop MultiPolygons (i.e. polygons not unified(two separate polygons))
    merged_single_parts = merged_geometries[merged_geometries.geom_type == "Polygon"]

    if merged_single_parts.empty:
        return None
    else:
        # Find the one with shortest boundary
        shortest_boundary_idx = merged_single_parts.length.idxmin()
        # Update geometry
        new_targets.loc[shortest_boundary_idx, "geometry"] = merged_geometries[
            shortest_boundary_idx
        ]
    return new_targets


def merge_by_longest_intersection(
    candidate: pd.Series, targets: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Merge a candidate parcel with the target that has the longest shared boundary.

    Attempts to merge the candidate with the target that shares the longest
    intersection line.

    Parameters
    ----------
    candidate : pd.Series
        Parcel to merge (must have 'geometry' attribute).
    targets : gpd.GeoDataFrame
        Candidate target parcels for merging.

    Returns
    -------
    gpd.GeoDataFrame or None
        Updated targets with candidate merged into the best match, or None if
        no valid intersection exists.
    """
    new_targets = targets.copy(deep=True)
    intersections = new_targets.geometry.apply(
        lambda x: shapely.intersection(candidate.geometry, x)
    )
    # Drop empty geometries
    proper_intersections = intersections[not intersections.is_empty]

    if proper_intersections.empty:
        return None
    else:
        longest_intersection_idx = proper_intersections.length.idxmax()
        # Update geometry
        new_geom = shapely.union(
            candidate.geometry,
            new_targets.loc[longest_intersection_idx, "geometry"],
        )
        new_targets.loc[longest_intersection_idx, "geometry"] = new_geom
    return new_targets


def get_contained_indices(
    gdf: gpd.GeoDataFrame, containing_geometry: shapely.geometry
) -> list:
    return gdf[gdf.geometry.within(containing_geometry)].index


def add_merged_geometries_property(
    gdf_merged: gpd.GeoDataFrame, gdf_original: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    gdf_merged.loc[:, MERGED_GEOM_PROPERTY] = None
    for idx in gdf_merged.index:
        merged_geometries = get_contained_indices(
            gdf_original, gdf_merged.loc[idx, "geometry"]
        )
        if len(merged_geometries) > 1:
            gdf_merged.at[idx, MERGED_GEOM_PROPERTY] = list(merged_geometries)
    return gdf_merged


def merge_geometries_by_criteria(
    candidates: gpd.GeoDataFrame,
    targets: gpd.GeoDataFrame,
    criteria: Optional[MergingCriteria] = MergingCriteria.SHORTEST_BOUNDARY,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    updated_candidates = candidates.copy(deep=True)
    updated_targets = targets.copy(deep=True)
    while True:
        something_merged = False
        current_candidates = updated_candidates.copy(deep=True)

        for idx, candidate in current_candidates.iterrows():
            if criteria == MergingCriteria.SHORTEST_BOUNDARY:
                tmp_targets = merge_by_shortest_boundary(candidate, updated_targets)
            elif criteria == MergingCriteria.LONGEST_INTERSECTION:
                tmp_targets = merge_by_longest_intersection(candidate, updated_targets)

            if tmp_targets is not None:
                something_merged = True
                updated_candidates = updated_candidates.drop(idx)
                updated_targets = tmp_targets

        if not something_merged:
            break

    return updated_targets, updated_candidates


def update_index(
    gdf: gpd.GeoDataFrame, current_max_idx: int
) -> tuple[gpd.GeoDataFrame, int]:
    gdf_cp = gdf.copy(deep=True)
    new_index = []
    for idx in gdf_cp.index:
        if gdf.loc[idx, MERGED_GEOM_PROPERTY] is None:
            new_index.append(idx)
        else:
            new_index.append(current_max_idx + 1)
            current_max_idx += 1
    gdf.index = new_index
    return gdf, current_max_idx


def split_by_min_area(
    gdf: gpd.GeoDataFrame, min_area: float
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    gdf_lt = gdf[gdf.geometry.area / 10000 < min_area]
    gdf_ge = gdf[gdf.geometry.area / 10000 >= min_area]
    return gdf_lt, gdf_ge


def split_by_min_width(
    gdf: gpd.GeoDataFrame, min_width: float
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    buff = gdf.buffer(-min_width / 2)
    gdf_lt = gdf[buff.is_empty]
    gdf_ge = gdf[~buff.is_empty]
    return gdf_lt, gdf_ge


def split_by_rule(
    gdf: gpd.GeoDataFrame,
    min_area: Optional[float] = None,
    min_width: Optional[float] = None,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    if min_area is not None:
        gdf_lt_area, gdf_ge_area = split_by_min_area(gdf, min_area)
    if min_width is not None:
        gdf_lt_width, gdf_ge_width = split_by_min_width(gdf, min_width)

    if min_area is not None and min_width is not None:
        gdf_lt_idx_union = gdf_lt_area.index.union(gdf_lt_width.index)
        gdf_lt = gdf.loc[gdf_lt_idx_union]
        gdf_ge = gdf.drop(gdf_lt_idx_union)
    elif min_area is not None:
        gdf_lt = gdf_lt_area
        gdf_ge = gdf_ge_area
    elif min_width is not None:
        gdf_lt = gdf_lt_width
        gdf_ge = gdf_ge_width

    return gdf_lt, gdf_ge


def merge_geometries(
    gdf: gpd.GeoDataFrame,
    min_area: Optional[float] = None,
    min_width: Optional[float] = None,
    merging_criteria: Optional[MergingCriteria] = MergingCriteria.SHORTEST_BOUNDARY,
) -> gpd.GeoDataFrame:
    """
    Merge small parcels based on area and/or width thresholds.

    Small parcels (below min_area in hectares or min_width in meters) are
    iteratively merged with adjacent larger parcels. The merging strategy
    is determined by the merging_criteria parameter.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame with parcel geometries to process.
    min_area : float, optional
        Minimum area threshold in hectares. Parcels below this are candidates
        for merging.
    min_width : float, optional
        Minimum width threshold in meters. Parcels narrower than this are
        candidates for merging.
    merging_criteria : MergingCriteria, default=SHORTEST_BOUNDARY
        Strategy for selecting which parcels to merge together.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with small parcels merged. Contains 'merged_geometries'
        column indicating which original parcels were merged.

    Raises
    ------
    ValueError
        If both min_area and min_width are None.

    Notes
    -----
    At least one of min_area or min_width must be specified.
    """
    if min_area is None and min_width is None:
        err_msg = "Either min_area or min_width must be given"
        logger.error(err_msg)
        raise ValueError(err_msg)

    gdf_lt, gdf_ge = split_by_rule(gdf, min_area, min_width)

    if gdf_lt.empty:
        gdf_updated = gdf_ge
    else:
        # Try merging small geometries together
        gdf_lt_merged = merge_to_single_parts(gdf_lt)
        gdf_lt_merged = add_merged_geometries_property(gdf_lt_merged, gdf)
        max_index = gdf.index.max()
        gdf_lt_merged, max_index = update_index(gdf_lt_merged, max_index)

        # Add merged small geometries to large geometries if they are large enough
        gdf_lt, gdf_ge_updates = split_by_rule(gdf_lt_merged, min_area, min_width)

        if not gdf_ge_updates.empty:
            gdf_ge = pd.concat(
                [gdf_ge, gdf_ge_updates.dropna(axis=1)], ignore_index=False
            )

        # Try merging small geometries to large geometries
        gdf_ge, gdf_lt = merge_geometries_by_criteria(
            gdf_lt, gdf_ge, criteria=merging_criteria
        )
        if not gdf_lt.empty:
            gdf_updated = pd.concat([gdf_ge, gdf_lt], ignore_index=True)
        else:
            gdf_updated = gdf_ge

    gdf_updated = add_merged_geometries_property(gdf_updated, gdf)

    gdf_lt, _ = split_by_rule(gdf_updated, min_area, min_width)

    if not gdf_lt.empty:
        logger.info(f"These geometries are still less than threshold:{gdf_lt.index}")
    return gdf_updated
