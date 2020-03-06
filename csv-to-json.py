#!/usr/bin/env python

# This is written with python3 syntax

import csv
import os
import json

INPUT_FILE_NAME = 'lookup.csv'
INPUT_FILE_PATH = os.path.join(os.getcwd(), INPUT_FILE_NAME)
CSV_DELIMITER = ','

OUTPUT_FILE_NAME = 'lookup.json'
OUTPUT_FILE_PATH = os.path.join(os.getcwd(), OUTPUT_FILE_NAME)

LOOKUP_COL = "lookup_id"


# Each CSV line will be converted into a dictionary object, and pushed
# onto an array. This ensures that the generated ruby
# will have the same order as the lines in the CSV file.
array_of_ordered_dict = []


# function to convert the CSV into an array that contains a json-like
# dictionary for each line in the CSV file
def create_ordered_dict_from_input():
    with open(INPUT_FILE_PATH) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=CSV_DELIMITER)
        print("Reading %s" % INPUT_FILE_PATH)
        for row in csv_reader:
            array_of_ordered_dict.append(row)

        print("Finished reading %s" % INPUT_FILE_PATH)
        return array_of_ordered_dict;


# Convert the array of dictionary objects into a ruby dictionary.
# This will ensure fast lookup in the Logstash pipeline
def convert_array_of_ordered_dict_to_json(array_of_ordered_dict):

    print("Creating %s" % OUTPUT_FILE_PATH)
    f = open(OUTPUT_FILE_PATH, "w")

    # Create the json lookup table
    f.write("{\n")

    arr_len = len(array_of_ordered_dict)
    for idx, row in enumerate(array_of_ordered_dict):
        point_id = row[LOOKUP_COL]
        del row[LOOKUP_COL]

        # point_id is a dictionary key, with the json_dict as the value
        json_element = '"{0}" : {1}'.format(point_id, json.dumps(row))

        # If this is the last json element, then the dictionary should be closed rather than
        # adding a trailing comma.
        json_line = ''.join([json_element, "\n}"]) if (idx+1) == arr_len else ''.join([json_element, ",\n"])

        f.write(json_line)

    print("Finished writing %s" % OUTPUT_FILE_PATH)
    return 0


if __name__ == "__main__":
    array_of_ordered_dict = create_ordered_dict_from_input()
    ruby_hash = convert_array_of_ordered_dict_to_json(array_of_ordered_dict)