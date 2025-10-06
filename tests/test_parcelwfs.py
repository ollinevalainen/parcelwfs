import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import parcelwfs

qvidja_ec_geom = [
    [22.3913931, 60.295311],
    [22.3917056, 60.2951721],
    [22.3922131, 60.2949717],
    [22.3927016, 60.2948124],
    [22.3932251, 60.2946874],
    [22.3931117, 60.2946416],
    [22.3926039, 60.2944037],
    [22.3920127, 60.2941585],
    [22.3918447, 60.2940601],
    [22.391413, 60.2937852],
    [22.3908102, 60.2935286],
    [22.390173, 60.2933897],
    [22.389483, 60.2933106],
    [22.3890777, 60.293541],
    [22.3891442, 60.2936358],
    [22.3889863, 60.2940313],
    [22.3892131, 60.2941537],
    [22.3895462, 60.2942468],
    [22.3899066, 60.2944289],
    [22.3903881, 60.2946329],
    [22.3904738, 60.2948121],
    [22.3913931, 60.295311],
]

qvidja_country = "FI"
qvidja_polygon = Polygon(qvidja_ec_geom)
year = 2023
multiple_years = [2022, 2023]
qvidja_ec_lpis_parcel_id_2023 = "5730455963"
qvidja_ec_gsaa_parcel_id_2023 = "5730455963-2"
polygon = qvidja_polygon
lat = polygon.centroid.y
lon = polygon.centroid.x


class TestParcelWFS:
    def test_get_parcelwfs_by_id(self):
        parcelwfs_ids = ["FI"]
        for parcelwfs_id in parcelwfs_ids:
            pwfs = parcelwfs.ParcelWFS.get_by_id(parcelwfs_id)
            assert isinstance(pwfs, parcelwfs.ParcelWFS)
            assert pwfs.id == parcelwfs_id

    def test_get_parcels_by_reference_parcel_id(self):
        parcelwfs_fi = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        gsaa_parcels = parcelwfs_fi.get_gsaa_parcels_by_lpis_parcel_id(
            qvidja_ec_lpis_parcel_id_2023, year
        )
        assert isinstance(gsaa_parcels, gpd.GeoDataFrame)
        assert (
            gsaa_parcels[parcelwfs_fi.gsaa_properties.lpis_parcel_id][0]
            == qvidja_ec_lpis_parcel_id_2023
        )

    def test_get_gsaa_parcel_by_id(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        parcel = wfs.get_gsaa_parcel_by_id(qvidja_ec_gsaa_parcel_id_2023, 2023)
        assert isinstance(parcel, pd.Series)
        assert (
            parcel[wfs.gsaa_properties.lpis_parcel_id] == qvidja_ec_lpis_parcel_id_2023
        )

    def test_get_parcel_by_lat_lon(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        parcel = wfs.get_gsaa_parcel_by_lat_lon(lat, lon, year)
        assert isinstance(parcel, pd.Series)
        assert (
            parcel[wfs.gsaa_properties.lpis_parcel_id] == qvidja_ec_lpis_parcel_id_2023
        )
        return parcel

    def test_get_parcel_species_by_lat_lon(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        species_per_year = wfs.get_gsaa_parcel_species_by_lat_lon(lat, lon, year)
        assert isinstance(species_per_year, dict)
        assert species_per_year[year]["lpis_parcel_id"] == qvidja_ec_lpis_parcel_id_2023

    def test_get_parcel_species_by_agri_parcel_id(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        species_per_year = wfs.get_gsaa_parcel_species_by_gsaa_parcel_id(
            qvidja_ec_gsaa_parcel_id_2023, year
        )
        assert isinstance(species_per_year, dict)

    def test_species_information_from_parcel(self):
        parcel = self.test_get_parcel_by_lat_lon()
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        species_information = wfs.species_information_from_gsaa_parcel(parcel)
        assert isinstance(species_information, dict)
        assert species_information["lpis_parcel_id"] == qvidja_ec_lpis_parcel_id_2023

    def test_get_reference_parcel_by_reference_parcel_id(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        parcel = wfs.get_lpis_parcel_by_id(qvidja_ec_lpis_parcel_id_2023, 2023)
        assert isinstance(parcel, pd.Series)
        assert (
            parcel[wfs.lpis_properties.lpis_parcel_id] == qvidja_ec_lpis_parcel_id_2023
        )

    def test_get_reference_parcel_by_lat_lon(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        parcel = wfs.get_lpis_parcel_by_lat_lon(lat, lon, year)
        assert isinstance(parcel, pd.Series)
        assert (
            parcel[wfs.lpis_properties.lpis_parcel_id] == qvidja_ec_lpis_parcel_id_2023
        )

    def test_species_information_for_reference_parcel_id(self):
        wfs = parcelwfs.ParcelWFS.get_by_id(qvidja_country)
        species_information = wfs.species_information_for_lpis_parcel_id(
            qvidja_ec_lpis_parcel_id_2023, 2023
        )
        assert isinstance(species_information, dict)
        assert species_information["lpis_parcel_id"] == qvidja_ec_lpis_parcel_id_2023
