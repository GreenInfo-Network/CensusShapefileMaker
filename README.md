# CensusShapefileMaker

A utility to download census polygons and attribute tables, then merge them.

A recurring need we have had, is to download census areas (blocks) then join them to attribute tables (number of males, number of people, mean income). Doing this using ArcMap has proven tedious and error-prone: the fields come in with unintuitive names such as P6i1 instead of TotPop, dropping a column hangs for a few seconds and we drop the large majority of them for any given use case, adding a new "PctMale" field based on "Male divided by TotPop" involves several steps since you need to also account for division by zero.

So, this specific need distilled down into a single script: Run one command, come back 10 minutes later to the dataset waiting for you: renamed fields, extraneous fields dropped, tract attributes mapped down to the individual blocks...

#Requirements

It's tested under Windows, specifically the OSGeo4W shell, but should work under Unix (OSX?) as well.

This is a command-line Python script, so requires that you have Python in your PATH.

It uses ogr2ogr to do some of the translations, so you'll need that installed too. If you're using Windows, check out OSGeo4W and its interactive shell.

#Usage

Download a whole state:
```
python GenerateStateMapperData.py XX
```
Where XX is the two-letter postal code for the state, for example.

Download a single county:
```
python GenerateStateMapperData.py XX 123
```
Where XX is the two-letter postal code for the state, and 123 is the three-digit FIPS code for the County.

Examples:
```
python GenerateStateMapperData.py FL
python GenerateStateMapperData.py FL 003
```
#Some Assembly Required

This was meant for a specific use case, and your own needs will likely be for different fields, and perhaps for some processing steps to be skipped (adding the age groups to determine Youth attribute). But it's a good starting place.

Some of the idiosyncracies include:

* This fetches census block polygons, not tracts nor blockgroups.
* Median Household income (MHHINC) is had from ACS at the tract level, while other attributes are had at the block level. Thus, two datasets are used: 2013 ACS and 2010 decennial.
* The variables DECENNIAL_FIELD_NAMES and DECENNIAL_FIELD_LABELS determine what attributes will be downloaded.
* The ACS content are "downsampled" to individual blocks, though of course remain at their prioer coarser resolution.
* The DecennialMerger class does some math to remove some fields for the final output. Most notably this means creating the YOUTH attribute by adding together the 8 age-by-sex fields.

#Credits, Thanks, Shoutouts

Thanks to the US Census Bureau, of course. Their FTP site provides a no-nonsense way to download the polygon shapefiles.

Thank you, Missouri Census Data Center (MCDC) for Dexter. It makes downloading decennial and ACS data so easy and consistent!

