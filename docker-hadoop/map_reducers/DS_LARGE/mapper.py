#!/usr/bin/env python3
# -*-coding:utf-8 -*
"""mapper.py"""
import sys
key_column_idx = sys.argv[1]  # This variable holds index of corresponding dataset key column
if key_column_idx != "None":
    key_column_idx = int(key_column_idx)


value_column_idx = int(sys.argv[2])  # This variable holds index of corresponding dataset value column

# You can define by hands if you want

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if line !="":
        # split the line into words
        words = line.split(",")
        # increase counters
        key = key_column_idx if key_column_idx == "None" else words[key_column_idx]
        print('%s\t%s' % (key, words[value_column_idx]))


