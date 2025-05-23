from ruokavirasto_wfs.parcels import Parcel, PARCEL_SEP

qvidja_ec_reference_parcel_id = "5730455963"
qvidja_ec_parcel_id = f"2022-{qvidja_ec_reference_parcel_id}"


class TestParcel:
    def test_parcel_init(self):
        parcel = Parcel(qvidja_ec_parcel_id)
        assert parcel is not None

    def test_get_parcels_from_reference_parcel_id(self):
        parcels = Parcel.get_parcels_from_reference_parcel_id(
            qvidja_ec_reference_parcel_id, 2022
        )
        assert len(parcels) >= 1

    def test_get_merged_parcels_from_referennce_parcel_id_qvidja(self):
        parcels = Parcel.get_merged_parcels_from_referennce_parcel_id(
            qvidja_ec_reference_parcel_id, 2022, min_area=0.5, min_width=20
        )
        assert len(parcels) == 1

    def test_get_merged_parcels_from_referennce_parcel_id_granular_parcel(self):
        parcels = Parcel.get_merged_parcels_from_referennce_parcel_id(
            "0860442742", 2023, min_area=0.5, min_width=20
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
        ref_parcel, agri_parcels = Parcel.extract_parcels_from_parcel_id(
            id_with_agri_parcels
        )
        assert ref_parcel == qvidja_ec_reference_parcel_id
        assert len(agri_parcels) == 2
