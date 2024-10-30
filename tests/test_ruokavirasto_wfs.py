import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import ruokavirasto_wfs as ruokawfs

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

qvidja_polygon = Polygon(qvidja_ec_geom)
year = 2023
multiple_years = [2022, 2023]
qvidja_ec_reference_parcel_id_2023 = "5730455963"
qvidja_ec_agri_parcel_id_2023 = "5730455963-2"
polygon = qvidja_polygon
lat = polygon.centroid.y
lon = polygon.centroid.x


class TestRuokavirastoWFS:

    def test_get_parcels_by_reference_parcel_id(self):
        parcels = ruokawfs.get_parcels_by_reference_parcel_id(
            qvidja_ec_reference_parcel_id_2023, year
        )
        assert isinstance(parcels, gpd.GeoDataFrame)
        assert (
            parcels[ruokawfs.AgriParcelProperty.REFERENCE_PARCEL_ID][0]
            == qvidja_ec_reference_parcel_id_2023
        )

    def test_get_parcel_by_parcel_id(self):
        parcel = ruokawfs.get_parcel_by_parcel_id(qvidja_ec_agri_parcel_id_2023, year)
        assert isinstance(parcel, pd.Series)
        parcel[ruokawfs.AgriParcelProperty.REFERENCE_PARCEL_ID][
            0
        ] == qvidja_ec_reference_parcel_id_2023

    def test_get_parcel_by_lat_lon(self):
        parcel = ruokawfs.get_parcel_by_lat_lon(lat, lon, year)
        assert isinstance(parcel, pd.Series)
        assert (
            parcel[ruokawfs.AgriParcelProperty.REFERENCE_PARCEL_ID]
            == qvidja_ec_reference_parcel_id_2023
        )
        return parcel

    def test_get_parcel_species_by_lat_lon(self):
        species_per_year = ruokawfs.get_parcel_species_by_lat_lon(lat, lon, year)
        assert isinstance(species_per_year, dict)
        assert (
            species_per_year[year]["reference_parcel_id"]
            == qvidja_ec_reference_parcel_id_2023
        )

    def test_get_parcel_species_by_parcel_id(self):
        species_per_year = ruokawfs.get_parcel_species_by_parcel_id(
            qvidja_ec_agri_parcel_id_2023, year
        )
        assert isinstance(species_per_year, dict)
        assert year in species_per_year

    def test_get_parcel_species_by_parcel_id_multi_year(self):
        species_per_year = ruokawfs.get_parcel_species_by_parcel_id(
            qvidja_ec_agri_parcel_id_2023, multiple_years
        )
        assert isinstance(species_per_year, dict)
        # In year the test field had different agri_parcel_id
        assert 2022 not in species_per_year
        assert 2023 in species_per_year

    def test_species_information_from_parcel(self):
        parcel = self.test_get_parcel_by_lat_lon()
        species_information = ruokawfs.species_information_from_parcel(parcel)
        assert isinstance(species_information, dict)
        assert (
            species_information["reference_parcel_id"]
            == qvidja_ec_reference_parcel_id_2023
        )
