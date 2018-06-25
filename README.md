# Community Snow Observations API

## Endpoints

### /obs
Params:
  - limit (int) (default = 1000) - Maximum number of records to return
  - page (int) (default = 1) - Page number of results
  - start (int) default = 1427458000000) - Earliest unix timestamp (in milliseconds) to return results from
  - end (int) (default = current time) - Latest unix timestamp (in milliseconds) to return results from
  - region (str) (default = None) - Region to return results from. Can be specified as a series of coordinates separated by `|`, e.g. `<lat_1>,<long_1>|<lat_2>,<long_2>|<lat_3>,<long_3>` or as an [encoded polyline](https://developers.google.com/maps/documentation/utilities/polylinealgorithm)

### /snodas
Params:
  - lat (int) (default = None) - Latitude of SNODAS records to return
  - long (int) (default = None) - Longitude of SNODAS records to return
