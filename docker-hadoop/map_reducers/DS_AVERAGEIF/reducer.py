#!/usr/bin/env python3
# -*-coding:utf-8 -*
"""reducer.py"""

import sys

current_key = None

key = None
current_mean = None
current_count = None
# input comes from STDIN

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, mean, count = line.split('\t', 2)

    # convert count (currently a string) to int
    try:
        mean = int(mean)
        count = int(count)
    except Exception as e:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        sum_current_total = current_mean * current_count
        sum_total = mean * count
        current_count = current_count + count
        current_mean = (sum_current_total + sum_total) / current_count

    else:
        if current_key:
            # write result to STDOUT
            print('%s\t%s\t%s' % (current_key, current_mean, current_count))

        current_key = key
        current_mean = mean
        current_count = count

# do not forget to output the last word if needed!
if current_key == key:
    print('%s\t%s\t%s' % (current_key, current_mean, current_count))
