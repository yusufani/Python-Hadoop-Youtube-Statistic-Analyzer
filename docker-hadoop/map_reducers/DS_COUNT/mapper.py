#!/usr/bin/env python3
# -*-coding:utf-8 -*
"""mapper.py"""

import sys

key_column_idx = sys.argv[1]  # This variable holds index of corresponding dataset key column

if key_column_idx != "None":
    key_column_idx = int(key_column_idx)


value_column_idx = sys.argv[2]  # This variable holds index of corresponding dataset value column
if value_column_idx.lower() != "count":
    value_column_idx = int(value_column_idx)

# You can define by hands if you want

# input comes from STDIN (standard input)
for line in sys.stdin:
    # print("Linecik", line)
    # remove leading and trailing whitespace
    line = line.strip()
    if line != "":
        # print("line" , line)
        # split the line into words
        try:
            words = line.split(",")
            # increase counters
            key = key_column_idx if key_column_idx == "None" else words[key_column_idx]
            val = "1" if value_column_idx == "count" else words[value_column_idx]

            print('%s\t%s' % (key, val))
        except Exception as e :
            print(e)

