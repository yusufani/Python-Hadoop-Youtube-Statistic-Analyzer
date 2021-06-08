#!/usr/bin/env python3
# -*-coding:utf-8 -*
"""reducer.py"""

from operator import itemgetter
import sys



k = int(sys.argv[1])  # This variable holds k for largest k. element


current_word = None
current_list = []
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        if len(current_list) != k:
            current_list.append(count)
        else:
            if count > current_list[k-1] :
                current_list[k-1] = count
        current_list = sorted(current_list,reverse=True)
    else:
        if current_word:
            # write result to STDOUT
            print("INFO:",  current_word , current_list)
            if len(current_list) == k:
                print ('%s\t%s' % (current_word, current_list[k-1]))
            else:
                print('%s\t%s' % (current_word, "Not Enough Records"))

        current_list =[count]
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print("INFO:", current_word, current_list)
    if len(current_list) == k:
        print('%s\t%s' % (current_word, current_list[k - 1]))
    else:
        print('%s\t%s' % (current_word, "Not Enough Records"))
