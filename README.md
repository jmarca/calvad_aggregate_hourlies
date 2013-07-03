# Aggregate Hourlies

Refactoring the aggregation code out of geo_bbox.

`geo_bbox` these days is primarily there to create the hourly
summations of travel activity for each area (grid cell, county, etc).
This package exists to extract out all of the code necessary to
aggregate those hourly values up higher temporal aggregates---daily,
weekly, monthly.

It isn't really a hard problem, but it is a bit distinct rom the
problem of summing up all of the imputations in the first place.

Also I wanted to split out some tests.

# command line and RAM

to run the aggregate properly, use the following command line option
`--max_old_space_size=4096`.  For example:


    node --max_old_space_size=4096 lib/execute_aggregate_hourly.js -r /home/james/repos/jem/carbserver/public -d data6 -y 2009 -j 10
    

This is because the larger areas (South Coast air basin and air
district) are too big to fit in the standard 2G node process space.
