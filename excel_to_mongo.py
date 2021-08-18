#!/usr/bin/python3

import pandas as pd
import re, optparse, sys, yaml
from pandas.io import excel
from yaml.loader import SafeLoader
from pymongo import MongoClient

def process_excel(excelFile, headerCount):
    # Read the excel file
    headerRows = []
    headerCount = int(headerCount)
    if headerCount + 1 < 2:
        headerRows.append(headerCount)
    else:
        for i in range(0, headerCount):
            headerRows.append(i)
    excelData = pd.read_excel(excelFile, header=headerRows, keep_default_na=False)
    excelData.fillna("", inplace=True)

    # Sanitize Header Data
    unnamed_re = re.compile("^Unnamed:\s[\d\w\-\_]*")
    column_name_array = []
    for column_tuple in excelData.columns:
        if type(column_tuple) == tuple:
            temp_columns = [unnamed_re.sub('', column_name) for column_name in list(column_tuple)]
            new_column_name = "_".join(temp_columns)
            new_column_name = re.sub('\s', "_", new_column_name)
            new_column_name = new_column_name.lower()
            new_column_name = new_column_name[:-1] if new_column_name.endswith("_") else new_column_name
            column_name_array.append(new_column_name)
        else:
            temp_columns = re.sub("\s", "_", column_tuple).lower()
            column_name_array.append(temp_columns)

    # Set the new column names
    excelData = excelData.set_axis(column_name_array, axis=1)
    # print(excelData)

    # Reset index and create a dict to upload to MongoDB
    excelData.reset_index(inplace=True)
    excelDataDict = excelData.to_dict("records")

    # Fetch the DB Configuration
    with open('db.config.yml') as configFile:
        dbConfig = yaml.load(configFile, Loader=SafeLoader)

    # Write Data to MongoDB
    client=MongoClient(f"mongodb://{dbConfig['user']}:{dbConfig['password']}@{dbConfig['host']}:{dbConfig['port']}")
    db = client["ExcelData"]
    collection_name = excelFile[:-5].split("/")[-1]
    collection = db[collection_name]

    collection.insert_many(excelDataDict)


def main():
    parser = optparse.OptionParser("Add single sheet excel data to MongoDB.")
    parser.add_option("-x", "--xls", help="Excel Filename", dest="excelFile")
    parser.add_option("--head", help="Number of header rows. Default is 1", dest="headerCount", default=1)

    (options, args) = parser.parse_args()

    if not options.excelFile:
        parser.print_help()
        sys.exit(1)

    process_excel(options.excelFile, options.headerCount)


if __name__ == "__main__":
    main()