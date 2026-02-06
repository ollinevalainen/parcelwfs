from parcelwfs.parcels import Parcel, PARCEL_SEP, get_parcel_history

qvidja_ec_reference_parcel_id = "5730455963"
qvidja_ec_parcel_id = f"2022{PARCEL_SEP}{qvidja_ec_reference_parcel_id}"
parcelwfs_id = "FI"


class TestParcel:
    def test_parcel_init(self):
        parcel = Parcel(qvidja_ec_parcel_id, parcelwfs_id=parcelwfs_id)
        assert parcel is not None

    def test_get_gsaa_parcels_by_lpis_parcel_id(self):
        parcels = Parcel.get_gsaa_parcels_by_lpis_parcel_id(
            qvidja_ec_reference_parcel_id, 2022, parcelwfs_id=parcelwfs_id
        )
        assert len(parcels) >= 1

    def test_get_merged_parcels_from_referennce_parcel_id_qvidja(self):
        parcels = Parcel.get_merged_gsaa_parcels_from_lpis_parcel_id(
            qvidja_ec_reference_parcel_id,
            2022,
            parcelwfs_id=parcelwfs_id,
            min_area=0.5,
            min_width=20,
        )
        assert len(parcels) == 1

    def test_get_merged_parcels_from_referennce_parcel_id_granular_parcel(self):
        parcels = Parcel.get_merged_gsaa_parcels_from_lpis_parcel_id(
            "0860442742", 2023, parcelwfs_id=parcelwfs_id, min_area=0.5, min_width=20
        )
        assert len(parcels) > 1

    def test_extract_parcels_from_parcel_id(self):
        id_with_agri_parcels = (
            "2022"
            + PARCEL_SEP
            + qvidja_ec_reference_parcel_id
            + PARCEL_SEP
            + "1"
            + PARCEL_SEP
            + "2"
        )
        ref_parcel, agri_parcels = Parcel.extract_lpis_and_gsaa_from_parcel_id(
            id_with_agri_parcels
        )
        assert ref_parcel == qvidja_ec_reference_parcel_id
        assert len(agri_parcels) == 2

    def test_get_parcel_history(self):
        import geopandas as gpd

        gsaa_parcels_2022 = Parcel.get_merged_gsaa_dataframe_from_lpis_id(
            qvidja_ec_reference_parcel_id,
            2022,
            parcelwfs_id=parcelwfs_id,
            min_area=0.5,
            min_width=20,
        )
        gsaa_parcels_2023 = Parcel.get_merged_gsaa_dataframe_from_lpis_id(
            qvidja_ec_reference_parcel_id,
            2023,
            parcelwfs_id=parcelwfs_id,
            min_area=0.5,
            min_width=20,
        )
        gdf_compared = gpd.pd.concat(
            [gsaa_parcels_2022, gsaa_parcels_2023], ignore_index=True
        )
        gdf_compared.to_crs(3067, inplace=True)

        lpis_parcel_2023 = Parcel(
            f"2023{PARCEL_SEP}{qvidja_ec_reference_parcel_id}",
            parcelwfs_id=parcelwfs_id,
        )
        gdf_reference = gpd.GeoDataFrame(
            [
                lpis_parcel_2023.wfs.get_lpis_parcel_by_id(
                    lpis_parcel_2023.lpis_parcel_id,
                    lpis_parcel_2023.year,
                    output_crs=3067,
                )
            ]
        )

        parcel_history = get_parcel_history(
            gdf_reference,
            gdf_compared,
            reference_year=2023,
            reference_id_col="PERUSLOHKOTUNNUS",
            compared_id_col="parcel_id",
            compared_year_col="VUOSI",
            min_overlap_fraction=0.01,
            number_of_decimal_places=2,
        )
        assert len(parcel_history) == 1
        assert "history" in parcel_history[qvidja_ec_reference_parcel_id]
        assert len(parcel_history[qvidja_ec_reference_parcel_id]["history"]) > 0
