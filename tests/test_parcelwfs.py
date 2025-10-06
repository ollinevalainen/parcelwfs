import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import parcelwfs


test_year = 2023
test_multiple_years = [2022, 2023]


fi_country = "FI"
fi_polygon = Polygon(
    [
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
)
fi_lpis_parcel_id_2023 = "5730455963"
fi_gsaa_parcel_id_2023 = "5730455963-2"
fi_lat = fi_polygon.centroid.y
fi_lon = fi_polygon.centroid.x

dk_code = "DK"
dk_polygon = None
dk_lpis_parcel_id = "725173-38"
dk_gsaa_parcel_id = "1-0"
dk_lat = 6173722
dk_lon = 725310.0


test_data = {
    "FI": {
        "year": test_year,
        "multiple_years": test_multiple_years,
        "lpis_parcel_id": fi_lpis_parcel_id_2023,
        "gsaa_parcel_id": fi_gsaa_parcel_id_2023,
        "lat": fi_lat,
        "lon": fi_lon,
    },
    "DK": {
        "year": test_year,
        "multiple_years": test_multiple_years,
        "lpis_parcel_id": dk_lpis_parcel_id,
        "gsaa_parcel_id": dk_gsaa_parcel_id,
        "lat": dk_lat,
        "lon": dk_lon,
    },
}


class TestParcelWFS:
    test_countries = ["FI", "DK"]

    def test_get_parcelwfs_by_id(self):
        for parcelwfs_id in self.test_countries:
            pwfs = parcelwfs.ParcelWFS.get_by_id(parcelwfs_id)
            assert isinstance(pwfs, parcelwfs.ParcelWFS)
            assert pwfs.id == parcelwfs_id

    def test_get_gsaa_parcels_by_lpis_parcel_id(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            gsaa_parcels = wfs.get_gsaa_parcels_by_lpis_parcel_id(
                test_data[country]["lpis_parcel_id"], test_data[country]["year"]
            )
            assert isinstance(gsaa_parcels, gpd.GeoDataFrame)
            assert (
                gsaa_parcels[wfs.gsaa_properties.lpis_parcel_id][0]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_get_gsaa_parcel_by_id(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            parcel = wfs.get_gsaa_parcel_by_id(
                test_data[country]["gsaa_parcel_id"], test_data[country]["year"]
            )
            assert isinstance(parcel, pd.Series)
            assert (
                parcel[wfs.gsaa_properties.lpis_parcel_id]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_get_parcel_by_lat_lon(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            parcel = wfs.get_gsaa_parcel_by_lat_lon(
                test_data[country]["lat"],
                test_data[country]["lon"],
                test_data[country]["year"],
            )
            assert isinstance(parcel, pd.Series)
            assert (
                parcel[wfs.gsaa_properties.lpis_parcel_id]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_get_parcel_species_by_lat_lon(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            species_per_year = wfs.get_gsaa_parcel_species_by_lat_lon(
                test_data[country]["lat"],
                test_data[country]["lon"],
                test_data[country]["year"],
            )
            assert isinstance(species_per_year, dict)
            assert (
                species_per_year[test_data[country]["year"]]["lpis_parcel_id"]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_get_parcel_species_by_agri_parcel_id(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            species_per_year = wfs.get_gsaa_parcel_species_by_gsaa_parcel_id(
                test_data[country]["gsaa_parcel_id"], test_data[country]["year"]
            )
            assert isinstance(species_per_year, dict)
            assert (
                species_per_year[test_data[country]["year"]]["lpis_parcel_id"]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_species_information_from_parcel(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            parcel = wfs.get_gsaa_parcel_by_id(
                test_data[country]["gsaa_parcel_id"], test_data[country]["year"]
            )
            species_information = wfs.species_information_from_gsaa_parcel(parcel)
            assert isinstance(species_information, dict)
            assert (
                species_information["lpis_parcel_id"]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_get_lpis_parcel_by_id(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            parcel = wfs.get_lpis_parcel_by_id(
                test_data[country]["lpis_parcel_id"], test_data[country]["year"]
            )
            assert isinstance(parcel, pd.Series)
            assert (
                parcel[wfs.lpis_properties.lpis_parcel_id]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_get_lpis_parcel_by_lat_lon(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            parcel = wfs.get_lpis_parcel_by_lat_lon(
                test_data[country]["lat"],
                test_data[country]["lon"],
                test_data[country]["year"],
            )
            assert isinstance(parcel, pd.Series)
            assert (
                parcel[wfs.lpis_properties.lpis_parcel_id]
                == test_data[country]["lpis_parcel_id"]
            )

    def test_species_information_for_lpis_parcel_id(self):
        for country in self.test_countries:
            wfs = parcelwfs.ParcelWFS.get_by_id(country)
            species_information = wfs.species_information_for_lpis_parcel_id(
                test_data[country]["lpis_parcel_id"], test_data[country]["year"]
            )
            assert isinstance(species_information, dict)
            assert (
                species_information["lpis_parcel_id"]
                == test_data[country]["lpis_parcel_id"]
            )
