#!/usr/bin/env python3
# -*-coding:utf-8 -*
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count_min = 0
current_count_max = None
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, view_count = line.split('\t',1)

    # convert count (currently a string) to int
    try:
        view_count = int(view_count)
    except Exception as e:
        # count was not a number, so silently
        # ignore/discard this line
        print(e)
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        if current_count_min > view_count:
            #print(current_word ," min " ,current_count_min )
            current_count_min = view_count
        if current_count_max < view_count: current_count_max = view_count
    else:
        if current_word:
            # write result to STDOUT
            print ('%s\t%s\t%s' % (current_word, current_count_min,current_count_max))
        current_count_min = view_count
        current_count_max = view_count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print ('%s\t%s\t%s' % (current_word, current_count_min,current_count_max))
