#!/usr/bin/python3

import pandas as pd
from pymongo import MongoClient
from yaml.loader import SafeLoader
import optparse, sys, yaml

def readCsv(csvFilename):
    csvFile = pd.read_csv(csvFilename)
    return csvFile

def writeMongo(dataFrame, csvFilename):
    with open('db.config.yml') as configFile:
        dbConfig = yaml.load(configFile, Loader=SafeLoader)
    client=MongoClient(f"mongodb://{dbConfig['user']}:{dbConfig['password']}@{dbConfig['host']}:{dbConfig['port']}")
    db = client['CSVData']
    csvFilename = csvFilename[:-4]
    collection = db[csvFilename]

    dataFrame.reset_index(inplace=True)
    data_dict = dataFrame.to_dict("records")

    collection.insert_many(data_dict)

def main():
    parser = optparse.OptionParser("This script will convert your csv into a MongoDB collection.")
    parser.add_option("-c", "--csv", help="CSV Filename", dest="csvFile")

    (options, args) = parser.parse_args()

    if not options.csvFile:
        parser.print_help()
        sys.exit(1)

    writeMongo(readCsv(options.csvFile), options.csvFile)
    

if __name__ == "__main__":
    main()