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
