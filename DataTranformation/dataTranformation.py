# Tranforming data from GEOJSON to CSV

import json
import csv

json_file = 'sample2.json'
csv_file = 'sample2.csv'

def features_to_rows(feature, header):
    row = []
    for value in header:
        row.append(feature['properties'][value])

    if feature['geometry'] is not None:
        coordinate = feature['geometry']['coordinates']
        assert(len(coordinate) == 2)
        # Remove the assertion for the trains.json as it will have more than 1 set of co-ordinates
        row.extend(coordinate)
    return row

with open(json_file,'r') as geo_json:
    with open(csv_file,'w', newline='') as geo_csv:
        geo_data = json.load(geo_json)
        features = geo_data['features']

        csv_writer = csv.writer(geo_csv)

        is_header = True
        header = []

        for feature in features:
            if is_header:
                is_header = False
                header = list(feature['properties'].keys())
                header.extend(['Longitude','Latitude'])
                csv_writer.writerow(header)

            csv_writer.writerow(features_to_rows(feature,feature['properties'].keys()))