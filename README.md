# Interface for Finnish Food Authority's (Ruokavirasto) field parcel data

Functions to retrieve data from the Finnish Food Authority's (Ruokavirasto) WFS service: https://inspire.ruokavirasto-awsa.com/geoserver/wfs?request=getcapabilities

## Installation using pip

```console
pip install git+https://github.com/ollinevalainen/ruokavirasto_wfs.git
```


## Example usage
````python
import ruokavirasto_wfs as ruokawfs
lat = 60.2942642
lon = 22.3908939
year = 2022
multiple_years = [2022,2023]

parcel = ruokawfs.get_parcel_by_lat_lon(lat, lon, year)
species_per_year = ruokawfs.get_parcel_species_by_lat_lon(lat, lon, multiple_years)
````
**Some other useful functions:**

* `get_available_parcel_years()` for a list of available years.

* `get_parcels_by_reference_parcel_id(reference_parcel_id, year)`, where 
`reference:parcel_id` is a Finnish reference parcel ID (PERUSLOHKOTUNNUS).
Function returns all agricultural parcels(kasvulohko) within that reference parcel at that year.

## TODO
* Add docstrings and documentation
* Make tests that won't break in the future when the test years are no longer available