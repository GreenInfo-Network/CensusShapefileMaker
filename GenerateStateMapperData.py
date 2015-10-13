#!/bin/env python
"""
Command-line application to fetch census blocks, decennial census tables (block), and ACS income (tract) data
then merge them into a single Mapper-ready shapefile for the whole state. (or, prune it to an area of interest for a more localized mapper)

Usage:
    Basic, grab a whole state:
        GenerateStateMapperData.py XX
        Where XX is the two-letter postal code for the state, e.g. CA or VT.
    County, filter the state dataset to a single county, via FIPS code:
        GenerateStateMapperData.py XX NNN
        Where XX is the two-letter postal code for the state, e.g. CA or VT.
        And NNN is a three-digit county FIPS code, e.g. 031
"""

# state FIPS codes
# so the user can give a two-letter code (CA, UT) and we look up the FIPS for them
# the FIPS is used for most filenames on remote (government) services, though we often want to use a human-friendly name for target filenames and output
STATE_FIPS_CODES = {
    "AL" : "01",
    "AK" : "02",
    "AZ" : "04",
    "AR" : "05",
    "CA" : "06",
    "CO" : "08",
    "CT" : "09",
    "DE" : "10",
    "DC" : "11",
    "FL" : "12",
    "GA" : "13",
    "HI" : "15",
    "ID" : "16",
    "IL" : "17",
    "IN" : "18",
    "IA" : "19",
    "KS" : "20",
    "KY" : "21",
    "LA" : "22",
    "ME" : "23",
    "MD" : "24",
    "MA" : "25",
    "MI" : "26",
    "MN" : "27",
    "MS" : "28",
    "MO" : "29",
    "MT" : "30",
    "NE" : "31",
    "NV" : "32",
    "NH" : "33",
    "NJ" : "34",
    "NM" : "35",
    "NY" : "36",
    "NC" : "37",
    "ND" : "38",
    "OH" : "39",
    "OK" : "40",
    "OR" : "41",
    "PA" : "42",
    "RI" : "44",
    "SC" : "45",
    "SD" : "46",
    "TN" : "47",
    "TX" : "48",
    "UT" : "49",
    "VT" : "50",
    "VA" : "51",
    "WA" : "53",
    "WV" : "54",
    "WI" : "55",
    "WY" : "56",
}

# what years do we go for? decennial is of course every 10 years, while ACS is approximately annual
ACS_YEAR       = "2013"
DECENNIAL_YEAR = "2010"

# from the MCDC Dexter, the list of census variables
# and a corresponding list of how to relabel those variables
DECENNIAL_FIELD_NAMES  = [
    'esriid',
    'P4i3','P6i1','P6i2','P6i3','P6i4','P6i5','P6i6',
    'P12i3','P12i4','P12i5','P12i6','P12i27','P12i28','P12i29','P12i30',
]
DECENNIAL_FIELD_LABELS = [
    'GEOID',
    'Hispanic','TotPop','White','Black','Amerind','Asian','Hawpi',
    'agem1','agem2','agem3','agem4','agef1','agef2','agef3','agef4'
]

####################################################################################################################################################
####################################################################################################################################################

import os, sys, time, re
import urllib, urllib2, zipfile
import csv

class PolygonDownloader:
    def __init__(self,config):
        self.config = config
        self.url    = "ftp://ftp2.census.gov/geo/tiger/TIGER%sBLKPOPHU/tabblock%s_%s_pophu.zip" % (DECENNIAL_YEAR,DECENNIAL_YEAR,self.config['statefips'])
        self.target = os.path.basename(self.url)
    def main(self):
        self.download()
        self.unpack()
        self.strip()
        pass
    def download(self):
        remote = urllib2.urlopen(self.url)
        local  = open(self.target, 'wb')
        size   = int( remote.info().getheaders("Content-Length")[0] )
        print "    %s" % (self.url)
        print "    %s" % (self.target)

        downloaded = 0
        while True:
            block = remote.read(1048576)
            if not block:
                break
            downloaded += len(block)
            local.write(block)
            print "    %d MB of %d MB" % (downloaded/1048576 , size/1048576)
        local.close()
        remote.close()
    def unpack(self):
        # unpack the four amigos
        print "    Unpacking shapefile from %s" % self.target
        local = open(self.target, 'rb')
        zip   = zipfile.ZipFile(local)
        for name in zip.namelist():
            if os.path.splitext(name)[1] != '.shp' and os.path.splitext(name)[1] != '.shx' and os.path.splitext(name)[1] != '.dbf' and os.path.splitext(name)[1] != '.prj':
                continue
            zip.extract(name)
        local.close()

        # rename the 4 files to a given set of names: censusblocks.shp et al
        # Windows can't do an atomic overwrite, so we have to unlink the target files first in case they exist
        shp1 = os.path.basename( os.path.splitext(self.target)[0] ) + '.shp';
        shx1 = os.path.basename( os.path.splitext(self.target)[0] ) + '.shx';
        dbf1 = os.path.basename( os.path.splitext(self.target)[0] ) + '.dbf';
        prj1 = os.path.basename( os.path.splitext(self.target)[0] ) + '.prj';

        try:
            os.unlink("censusblocks_raw.shp")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_raw.shx")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_raw.dbf")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_raw.prj")
        except OSError:
            pass

        os.rename(shp1,"censusblocks_raw.shp")
        os.rename(shx1,"censusblocks_raw.shx")
        os.rename(dbf1,"censusblocks_raw.dbf")
        os.rename(prj1,"censusblocks_raw.prj")

        os.unlink(self.target)
        print "    Ready: censusblocks.shp"
    def strip(self):
        try:
            os.unlink("censusblocks.shp")
        except OSError:
            pass
        try:
            os.unlink("censusblocks.shx")
        except OSError:
            pass
        try:
            os.unlink("censusblocks.dbf")
        except OSError:
            pass
        try:
            os.unlink("censusblocks.prj")
        except OSError:
            pass

        print "    Stripping extraneous fields from polygons"
        command = 'ogr2ogr -sql "SELECT BLOCKID10 AS GEOID FROM censusblocks_raw" censusblocks.shp censusblocks_raw.shp'
        os.system(command)

        try:
            os.unlink("censusblocks_raw.shp")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_raw.shx")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_raw.dbf")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_raw.prj")
        except OSError:
            pass


class DecennialDownloader:
    def __init__(self,config):

        dataset   = "%sblocks" % ( config['state'].lower() )
        params    = {
            "sasdset" : dataset,
            "_PROGRAM" : "websas.dexter.sas",
            "_SERVICE" : "bigtime",
            "path" : "/pub/data/sf1%s" % DECENNIAL_YEAR,
            "view" : "0",
            "ranksteropt" : "no",
            "quicklook" : "0",
            "dlm" : "csv",
            "lst" : "none",
            "dbfile" : "none",
            "fkey1" : "",
            "op1" : "",
            "value1" : "",
            "logic1" : "and",
            "fkey2" : "",
            "op2" : "",
            "value2" : "",
            "logic2" : "and",
            "fkey3" : "",
            "op3" : "",
            "value3" : "",
            "logic3" : "and",
            "fkey4" : "",
            "op4" : "",
            "value4" : "",
            "logic4" : "and",
            "fkey5" : "",
            "op5" : "",
            "value5" : "",
            "maxobs" : "",
            "_subtype" : "ph",
            "varlist" : ",".join(DECENNIAL_FIELD_NAMES),
            "title2" : "",
            "subtitle" : " ",
            "footnote" : " ",
            "sortby" : "",
            "_aggby" : "",
            "_agglvl" : "1",
            "_grand" : "NO",
            "_means" : "",
            "_mweights" : "",
            "_dropvars" : "_lvl_  _nag_",
            "rept_byvars" : "",
            "rept_idvars" : "",
            "orient" : "portrait",
            "odsstyle" : "sasweb",
            "fmtstmt" : "",
            "renamestmt" : "",
            "labelstmt" : "",
            "transby" : "",
            "transprefix" : "",
            "transname" : "",
            "transidlabel" : "",
            "hasxys" : "1",
            "haslookupvars" : "1",
            "hasgeoid" : "0",
            "latvar" : "IntPtLat",
            "lonvar" : "IntPtLon",
            "_y0lat" : "",
            "_x0long" : "",
            "_debug" : "",
            "query" : "",
        }
        self.url = "http://mcdc.missouri.edu/cgi-bin/broker?%s" % urllib.urlencode(params)
    def main(self):
        self.download()
        self.massage()
    def download(self):
        # open the URL we figured out earlier; hooray for Dexter allowing both GET and POST !
        print "    Requesting decennial data from Dexter"
        print "    %s" % self.url
        content = urllib2.urlopen(self.url)
        content = content.read()
        url     = "http://mcdc.missouri.edu/" + re.search(r'(/tmpscratch/[\w\.]+/xtract.csv)',content).groups()[0]
        print "    Ready: %s" % url
        urllib.urlretrieve(url, "decennial_attributes_raw.csv")
        print "    Downloaded decennial_attributes_raw.csv"
    def massage(self):
        # trim the first 2 rows, replace them with one with good field names
        # then just pass the other rows through as is
        print "    Fixing data in decennial_attributes_raw.csv"
        csvread  = open('decennial_attributes_raw.csv', 'rb')
        csvwrite = open('decennial_attributes.csv', 'wb')

        csvoutput = csv.writer(csvwrite)
        csvinput  = csv.reader(csvread)

        csvoutput.writerow(DECENNIAL_FIELD_LABELS)

        csvinput.next()
        csvinput.next()
        for row in csvinput:
            csvoutput.writerow(row)

        csvread.close()
        csvwrite.close()
        print "    Done: decennial_attributes.csv"
        os.unlink('decennial_attributes_raw.csv')


class ACSDownloader:
    def __init__(self,config):
        params = {
            "sasdset" : "ustracts5yr",
            "_PROGRAM" : "websas.dexter.sas",
            "_SERVICE" : "bigtime",
            "path" : "/pub/data/acs%s" % ACS_YEAR,
            "view" : "0",
            "ranksteropt" : "no",
            "quicklook" : "0",
            "dlm" : "csv",
            "lst" : "none",
            "dbfile" : "none",
            "fkey1" : "",
            "op1" : "",
            "value1" : "",
            "logic1" : "and",
            "fkey2" : "",
            "op2" : "",
            "value2" : "",
            "logic2" : "and",
            "fkey3" : "",
            "op3" : "",
            "value3" : "",
            "logic3" : "and",
            "fkey4" : "",
            "op4" : "",
            "value4" : "",
            "logic4" : "and",
            "fkey5" : "",
            "op5" : "",
            "value5" : "",
            "maxobs" : "",
            "idv" : "esriid",
            "_subtype" : " ",
            "v" : "AvgHHInc",
            "vregexp" : "",
            "varlist" : "esriid,MedianHHInc",
            "title2" : "",
            "subtitle" : " ",
            "footnote" : " ",
            "sortby" : "",
            "_aggby" : "",
            "_agglvl" : "1",
            "_grand" : "NO",
            "_means" : "",
            "_mweights" : "",
            "_dropvars" : "_lvl_  _nag_",
            "rept_byvars" : "",
            "rept_idvars" : "",
            "orient" : "portrait",
            "odsstyle" : "sasweb",
            "fmtstmt" : "",
            "renamestmt" : "",
            "labelstmt" : "",
            "transby" : "",
            "transprefix" : "",
            "transname" : "",
            "transidlabel" : "",
            "hasxys" : "1",
            "haslookupvars" : "1",
            "hasgeoid" : "1",
            "latvar" : "IntPtLat",
            "lonvar" : "IntPtLon",
            "_y0lat" : "",
            "_x0long" : "",
            "_debug" : "",
            "query" : "",
        }
        self.url = "http://mcdc.missouri.edu/cgi-bin/broker?%s" % urllib.urlencode(params)
    def main(self):
        self.download()
        self.massage()
    def download(self):
        # open the URL we figured out earlier; hooray for Dexter allowing both GET and POST !
        print "    Requesting ACS data from Dexter"
        content = urllib2.urlopen(self.url)
        content = content.read()
        url     = "http://mcdc.missouri.edu/" + re.search(r'(/tmpscratch/[\w\.]+/xtract.csv)',content).groups()[0]
        print "    Ready: %s" % url
        urllib.urlretrieve(url, "acs_attributes_raw.csv")
        print "    Downloaded acs_attributes_raw.csv"
    def massage(self):
        # trim the first 2 rows, replace them with one with good field names
        # then fix the rows: dollar values have $ and commas, and nulls are empty cells which botch the math
        print "    Fixing data in acs_attributes_raw.csv"
        csvread  = open('acs_attributes_raw.csv', 'rb')
        csvwrite = open('acs_attributes.csv', 'wb')

        csvoutput = csv.writer(csvwrite)
        csvinput  = csv.reader(csvread)

        csvoutput.writerow(['TRACTID','MHHINC'])

        csvinput.next()
        csvinput.next()
        for geoid,dollars in csvinput:
            if dollars:
                dollars = int( dollars.replace('$','').replace(',','') )
            else:
                dollars = 0
            csvoutput.writerow([geoid,dollars])

        csvread.close()
        csvwrite.close()
        print "    Done: acs_attributes.csv"
        os.unlink('acs_attributes_raw.csv')


class ACSMerger():
    def __init__(self,config):
        # load the ACS CSV file into a giant assoc keyed by tract ID
        print "    Loading acs_attributes.csv into memory"
        self.tract_attributes = {}
        csvread  = open('acs_attributes.csv', 'rb')
        csvinput = csv.reader(csvread)
        csvinput.next() # skip the first line
        for tractid,mhhinc in csvinput:
            self.tract_attributes[tractid] = { 'MHHINC':int(mhhinc) }
        csvread.close()

    def main(self):
        print "    Assigning ACS fields to records in censustracts.shp"

        # 1 - open the shapefile and add attributes to it (unless it exists already)
        source = ogr.Open('censusblocks.shp', 1)
        layer  = source.GetLayer()
        defn   = layer.GetLayerDefn()

        if defn.GetFieldIndex('MHHINC') == -1:
            layer.CreateField( ogr.FieldDefn('MHHINC', ogr.OFTInteger) )

        # 2 - then iterate over all records in it, and populate them from the ACS data we cached during init
        # the tract-ID is the first 11 characters of the GEOID, we can key from that
        feature = layer.GetNextFeature()
        while feature:
            tractid    = feature.GetField("GEOID")[:11]
            attributes = self.tract_attributes[tractid]
            feature.SetField("MHHINC", attributes['MHHINC'] )
            layer.SetFeature(feature)
            feature = layer.GetNextFeature()

        # 999 - done
        source.Destroy()


class DecennialMerger():
    def __init__(self,config):
        # load the ACS CSV file into a giant assoc keyed by tract ID
        print "    Loading decennial_attributes.csv into memory"
        self.block_attributes = {}
        csvread  = open('decennial_attributes.csv', 'rb')
        csvinput = csv.reader(csvread)
        csvinput.next() # skip the first line
        for geoid,hispanic,totalpop,white,black,amerind,asian,hawpi,age1,age2,age3,age4,age5,age6,age7,age8 in csvinput:
            self.block_attributes[geoid] = {
                'HISP':int(hispanic),
                'TOTPOP':int(totalpop),
                'WHITE':int(white),
                'BLACK':int(black),
                'AMERIND':int(amerind),
                'ASIAN':int(asian),
                'HAWPI':int(hawpi),
                'YOUTH':int(age1) + int(age2) + int(age3) + int(age4) + int(age5) + int(age6) + int(age7) + int(age8),
            }
        csvread.close()

    def main(self):
        print "    Assigning ACS fields to records in censustracts.shp"

        # 1 - open the shapefile and add attributes to it (unless it exists already)
        source = ogr.Open('censusblocks.shp', 1)
        layer  = source.GetLayer()
        defn   = layer.GetLayerDefn()

        if defn.GetFieldIndex('HISP') == -1:
            layer.CreateField( ogr.FieldDefn('HISP', ogr.OFTInteger) )
        if defn.GetFieldIndex('TOTPOP') == -1:
            layer.CreateField( ogr.FieldDefn('TOTPOP', ogr.OFTInteger) )
        if defn.GetFieldIndex('WHITE') == -1:
            layer.CreateField( ogr.FieldDefn('WHITE', ogr.OFTInteger) )
        if defn.GetFieldIndex('BLACK') == -1:
            layer.CreateField( ogr.FieldDefn('BLACK', ogr.OFTInteger) )
        if defn.GetFieldIndex('AMERIND') == -1:
            layer.CreateField( ogr.FieldDefn('AMERIND', ogr.OFTInteger) )
        if defn.GetFieldIndex('ASIAN') == -1:
            layer.CreateField( ogr.FieldDefn('ASIAN', ogr.OFTInteger) )
        if defn.GetFieldIndex('HAWPI') == -1:
            layer.CreateField( ogr.FieldDefn('HAWPI', ogr.OFTInteger) )
        if defn.GetFieldIndex('YOUTH') == -1:
            layer.CreateField( ogr.FieldDefn('YOUTH', ogr.OFTInteger) )

        # 2 - then iterate over all records in it, and populate them from the ACS data we cached during init
        feature = layer.GetNextFeature()
        while feature:
            geoid    = feature.GetField("GEOID")
            attributes = self.block_attributes[geoid]
            feature.SetField("HISP",    attributes['HISP'] )
            feature.SetField("TOTPOP",  attributes['TOTPOP'] )
            feature.SetField("WHITE",   attributes['WHITE'] )
            feature.SetField("BLACK",   attributes['BLACK'] )
            feature.SetField("AMERIND", attributes['AMERIND'] )
            feature.SetField("ASIAN",   attributes['ASIAN'] )
            feature.SetField("HAWPI",   attributes['HAWPI'] )
            feature.SetField("YOUTH",   attributes['YOUTH'] )
            layer.SetFeature(feature)
            feature = layer.GetNextFeature()

        # 999 - done
        source.Destroy()


class CountyTrimmer:
    def __init__(self,config):
        self.statefips  = config['statefips']
        self.countyfips = config['countyfips']
    def main(self):
        # unlink the target "trimmed" shapefile, since ogr2ogr will pitch a fit if it already exists
        try:
            os.unlink("censusblocks_trimmed.shp")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_trimmed.shx")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_trimmed.dbf")
        except OSError:
            pass
        try:
            os.unlink("censusblocks_trimmed.prj")
        except OSError:
            pass

        command = 'ogr2ogr -sql "SELECT * FROM censusblocks WHERE SUBSTR(GEOID,1,5) = \'%s%s\'" censusblocks_trimmed.shp censusblocks.shp' % (self.statefips,self.countyfips)
        os.system(command)

        # now rename "trimmed" and "non-trimmed" into place
        os.rename('censusblocks.shp','censusblocks_statewide.shp')
        os.rename('censusblocks.shx','censusblocks_statewide.shx')
        os.rename('censusblocks.dbf','censusblocks_statewide.dbf')
        os.rename('censusblocks.prj','censusblocks_statewide.prj')

        os.rename('censusblocks_trimmed.shp','censusblocks.shp')
        os.rename('censusblocks_trimmed.shx','censusblocks.shx')
        os.rename('censusblocks_trimmed.dbf','censusblocks.dbf')
        os.rename('censusblocks_trimmed.prj','censusblocks.prj')


####################################################################################################################################################
####################################################################################################################################################

if __name__ == '__main__':
    # sanity checks: make sure we have OGR module
    try:
        from osgeo import ogr
    except:
        print "Could not import ogr"
        print "You must have Python-OGR installed, e.g. be using Python from OSG4Win"
        sys.exit(4)

    # sanity checks: make sure we can execute ogr2ogr
    command = 'ogr2ogr --version'
    code = os.system(command)
    if code != 0:
        print "Could not execute ogr2ogr binary. Are you sure it's installed?"
        sys.exit(2)

    # parse command-line params, there's only 1
    try:
        state  = sys.argv[1]
        try:
            county = sys.argv[2]
            if len(county) != 3:
                print "ERROR: Bad county FIPS code; County FIPS are always 3 digits."
                print ""
                sys.exit(1)
        except IndexError:
            county = None
        fips   = STATE_FIPS_CODES[state]
    except:
        print "USAGE"
        print "Whole state:"
        print "    python GenerateStateMapperData.py XX"
        print "        Where XX is the two-letter postal code for the state, e.g. CA or VT."
        print "Single county by FIPS code:"
        print "    python GenerateStateMapperData.py XX NNN"
        print "        Where XX is the two-letter postal code for the state, e.g. CA or VT."
        print "        And NNN is a three-digit county FIPS code, e.g. 031"
        sys.exit(1)

    # compose a single config object, and tell the user what we think they asked for
    config = { 'state':state, 'statefips':fips, 'countyfips':county }
    print "Preparing to generate data for %s   State FIPS code is %s" % (config['state'],config['statefips'])
    if county:
        print "Will filter by county FIPS = %s" % county
    print ""
    print "If that looks right, just wait 5 seconds."
    print "If not, hit ctrl-C right now to abort."
    time.sleep(5)
    print "Starting"
    print ""

    # Part 1: download all the datasets: polygon geometries, ACS for income data at tract level, decennial census for race and youth attributes
    print "Fetching polygon shapefile from USCB FTP"
    plyd = PolygonDownloader(config)
    plyd.main()

    print "Fetching decennial attributes from MCDC Dexter"
    decd = DecennialDownloader(config)
    decd.main()

    print "Fetching ACS income tract attributes from MDCDC Dexter"
    acsd = ACSDownloader(config)
    acsd.main()

    # Part 2: merge the attributes from the CSVs into the shapefile via OGR
    print "Merging ACS MHHINC into censusblocks"
    merge1 = ACSMerger(config)
    merge1.main()

    print "Merging Decennial attribs into censusblocks"
    merge2 = DecennialMerger(config)
    merge2.main()

    # Part 3: if a specific county FIPS was given then filter the censusblocks shapefile
    # yeah, in postprocessing after we we wasted time doing joins earlier, but it was seconds and this is best left in a final massage phase, so we can abort and examine the not-yet-pruned version when debugging
    if config['countyfips']:
        print "Trimming shapefile to county FIPS %s" % config['countyfips']
        trimmer = CountyTrimmer(config)
        trimmer.main()
