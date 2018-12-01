# Convert new GeoIp format back to legacy format

As MaxMind has stopped updating the legacy format CSV files in March 2018 and now offers a new CSV format, I need to convert the new format back to the legacy so that we can obtain the same format as input for applications that would be difficult to update. I'm really just interested in CSV for countries (not cities or regions) that is named `GeoIPCountryWhois.csv`.

The legacy format that we use looks like this:
* start IP address
* end IP address
* start IP address (as an integer)
* end IP address (as an integer)
* country ISO code
* country name in English

A sample line looks like this (yes, all values are quoted):

|start IP|end IP|start IP int|end IP int|iso code|country name|
|---|---|---|---|---|---|
|"1.0.0.0" | "1.0.0.255" | "16777216" | "16777471" | "AU" | "Australia" |

The new format uses Geoname IDs as primary keys in a specific locations file and which are referenced in the files that list the IP address ranges. Here is the first line of the locations file (the English version):

|geoname_id|locale_code|continent_code|continent_name|country_iso_code|country_name|is_in_european_union|
|---|---|---|---|---|---|---|
|49518|en|AF|Africa|RW|Rwanda|0|


Also, the new format uses CIDR ranges which need to be translated to the start/end IP address fields.

|network|geoname_id|registered_country_geoname_id|represented_country_geoname_id|is_anonymous_proxy|is_satellite_provider|
|---|---|---|---|---|---|
|1.0.0.0/24|2077456|2077456||0|0|

