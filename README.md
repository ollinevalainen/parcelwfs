# Interface for retrieving field parcel data from national GSAA and LPIS WFS services

Functions to retrieve agricultural parcel data from national GSAA (Geospatial Aid applications) and LPIS (Land Parcel Identification System) WFS services. Currently implement for Finland and Denmark, but can be extended to other countries with similar services.

## Installation using pip

```console
pip install git+https://github.com/ollinevalainen/parcelwfs.git
```


## Example usage
````python
import parcelwfs
lat = 60.2942642
lon = 22.3908939
year = 2022
multiple_years = [2022,2023]
wfs = parcelwfs.ParcelWFS.get_by_id("FI")
parcel = wfs.get_gsaa_parcel_by_lat_lon(lat, lon, year)
````
## Adding new countries
To add a new country, create a new YAML configuration file in the `parcelwfs` directory, following the structure of the existing `FI.yaml` and `DK.yaml` files. The configuration should include the WFS service URL, layer names, and property mappings for both GSAA and LPIS data.

The configuration file for Finland (FI.yaml) looks like this:
```yaml
id: FI
endpoints: 
  gsaa: https://inspire.ruokavirasto-awsa.com/geoserver/wfs
  lpis: https://inspire.ruokavirasto-awsa.com/geoserver/wfs
layers:
  gsaa: inspire:LandUse.ExistingLandUse.GSAAAgriculturalParcel.
  lpis: inspire:LC.LandCoverSurfaces.LPIS.
gsaa_properties:
    id: id
    year: VUOSI
    lpis_parcel_id: PERUSLOHKOTUNNUS
    species_code: KASVIKOODI
    species_description: KASVIKOODI_SELITE_FI
    area: PINTA_ALA
    gsaa_parcel_name: LOHKONUMERO
    geometry: geom
lpis_properties:
    id: id
    year: VUOSI
    lpis_parcel_id: PERUSLOHKOTUNNUS
    area: PINTA_ALA
    geometry: geom
wfs_version: 2.0.0
```

You can use custom YAML file path when initializing `ParcelWFS` class:
```python
import parcelwfs
wfs = parcelwfs.ParcelWFS.from_yaml("path/to/custom/config.yaml")
```

Or you can generate a `ParcelWFS` instance directly from parameters:
```python
import parcelwfs
wfs = parcelwfs.ParcelWFS.model_validate(
    {
        "id": "XX",
        "endpoints": {
            "gsaa": "https://example.com/gsaa/wfs",
            "lpis": "https://example.com/lpis/wfs"
        },
        "layers": {
            "gsaa": "example:GSAA_Layer",
            "lpis": "example:LPIS_Layer"
        },
        "gsaa_properties": {
            "id": "id",
            "year": "year",
            "lpis_parcel_id": "lpis_id",
            "species_code": "species_code",
            "species_description": "species_desc",
            "area": "area",
            "gsaa_parcel_name": "parcel_name",
            "geometry": "geom"
        },
    "lpis_properties": {
        "id": "id",
        "year": "year",
        "lpis_parcel_id": "lpis_id",
        "area": "area",
        "geometry": "geom"
    },
    "wfs_version": "2.0.0"
    }
)
```

## TODO
* Improve documentation
* Test parcels with Danish data
* Add more countries