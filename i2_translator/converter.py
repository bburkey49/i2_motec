import pandas as pd
import csv
# from headers import headers
from ldparser.ldparser import *


influxDB_file_delimiter = '\n'
strip_chars = '[]\''
protocol_names = [
    'measurement',
    'tag-set',
    'field-set',
    'timestamp'
]

pdfs = {}


def parse_influxDB(data_point_string):
    i = 0
    parsed_influxDB = {}
    for name in protocol_names[:3]:
        if name == 'measurement':
            delim = ','
        else:
            delim = ' '
        p_index = data_point_string.index(delim, i, len(data_point_string))
        parsed_influxDB.update({
            name : data_point_string[i:p_index]
        })
        i = p_index + 1

    parsed_influxDB.update({
            'timestamp' : data_point_string[i:len(data_point_string)]
    })
    return parsed_influxDB


def retrieve_string_items(sep, key_value_string):
    i = 0
    items = []
    curr_str = key_value_string

    while len(curr_str) > 0:
        item, lst = curr_str.partition(sep)[0], curr_str.partition(sep)[2]
        curr_str = lst
        items.append(item)
    return items


def parse_for_cols(col_string):
    col_dict = {}
    items = retrieve_string_items(',', col_string)
    for item in items:
        k,v = item.partition('=')[0], item.partition('=')[2]
        col_dict.update({
            k : float(v)
        })
    return col_dict



with open('shared_test_data/sample_data.txt') as influxDB_file:
    fr = csv.reader(influxDB_file, delimiter = influxDB_file_delimiter)
    data = []
    for dps in list(fr):
        dp_dict = parse_influxDB(str(dps).strip(strip_chars))
        col_dict = {}
        col_dict.update(parse_for_cols(dp_dict['tag-set']))
        col_dict.update(parse_for_cols(dp_dict['field-set']))

        headers = list(col_dict.keys())
        data.append(list(col_dict.values()))


    new_data = pd.DataFrame(data = list(data), columns = headers)
    print(new_data)

    ld = ldData.frompd(new_data)
    print(ld)
    for l in ld:
        print(l)
    ld.write('translated.ld')








