#!/usr/bin/env python3
# -*-coding:utf-8 -*
"""mapper.py"""
import sys

key_column_idx = sys.argv[1]  # This variable holds index of corresponding dataset key column
if key_column_idx != "None":
    key_column_idx = int(key_column_idx)

value_column_idx = int(sys.argv[2])  # This variable holds index of corresponding dataset value column

compare_col1_idx = int(sys.argv[3])
compare_operator = sys.argv[4]
compare_col2_idx = int(sys.argv[5])


# You can define by hands if you want

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if line != "":
        # split the line into words
        words = line.split(",")

        if eval(words[compare_col1_idx] +  compare_operator  + words[compare_col2_idx]):
            # increase counters
            key = key_column_idx if key_column_idx == "None" else words[key_column_idx]
            print('%s\t%s\t1' % (key, words[value_column_idx]))



    """
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print ('%s\t%s' % (word, 1))
    """
