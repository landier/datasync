# Data Extractor

# Quickstart
```bash
cd datasync
make
```

# Mode
There are two different modes:
* __daemon__ (default): which starts an hourly incremental sync
* __full__: which triggers a full sync

# Notes
* Without prior knowledge of Snowflake, going through the documentation had a significant impact of the time I had to dedicate to this project.
* Latest snowflake-python-connector is not working on Python 3.7 (SSL bug). Had to fix the library version to 2.0.2.
* I first thought the API was outputting invalid JSON but it turned out the API is actually outputting Python object... spent too much times trying to cleanse what I thought was invalid JSON with things like:
```python
invalid_text = invalid_text.replace("\'", "\"")
invalid_text = invalid_text.replace('\\xa0', "")
invalid_text = invalid_text.replace("None", "null")
invalid_text = invalid_text.replace("False", "false")
invalid_text = invalid_text.replace("True", "true")
```
* __Rate limit consideration__: I understand the "weird" behavior of negative cost on high pages which is somehow due to a potential different technical storage backend over 100 pages. We can sort (ascending and desceding) on the different fields in order to build a complete dataset without going over 100 pages. The drawback of this method is we'll issue multiple queries to fetch the same data: which is usually something we don't want since this is usually what is billed/considered for rate limit.

## Todo
- [ ] Verify records timezones on API: API supports timezone, are we filtering out some records because of the filter or are all records stored with UTC+2 (hence we should be using the UTC+2 to filter on API)?
- [ ] Do not update data in DB if last_update date in DB is the same as from the API
- [ ] Make the APIClient.fetch method a generator to stream changes to the DAL
- [ ] Add an intermediary local storage of records to be able to re-play without fetching again all the data and support retry strategy in a production environment
- [ ] Multithreading / multiprocessing
