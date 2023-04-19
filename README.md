# Uploads spetial_measures

The upload spatial measures project would be used as a tool for updating the GEO metrics information about our pipeline.
It will took the TIFF and convert it to the right GEO format if necessary and change the unit of values from kilometers to meters if required. At the end will convert the values in `XYZ` format to be easily readable in a table. It will upload the data to the table and will create the look up table.

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

IMPORTANT: This script is interactive.
In order to execute the tool use:

```
docker compose run --rm -it upload_spatial_measures upload_measures -i distance_to_port_v20230302_001.tiff
```
It will output the file in meters: `distance_to_port_v20230302_001_m.tiff` and the distance from port file in format `XYZ`.



