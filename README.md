**Interface for Finnish Food Authority's (Ruokavirasto) field parcel data**

Example usage:
````python
import ruokavirasto_wfs as ruokawfs
# Qvidja eddy-covariance field
lat = 60.2942642
lon = 22.3908939
year = 2022

gdf_parcel = ruokawfs.get_parcel_by_lat_lon(lat, lon, year)
species_dict = ruokawfs.get_parcel_species(lat, lon, year)
````
Some other useful functions:

`get_available_parcel_years()` for a list of available years.

`get_parcel_by_parcel_id(parcel_id, year)`, where `parcel_id` is a Finnish field parcel ID (PERUSLOHKOTUNNUS).
