#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Quick script to output stats from a cProfile results file
#  
#  Other wise install kcachegrind and pyprof2calltree and use:
#     pyprof2calltree -i profile_data.pyprof -k
#  
#  Use
#    pypy -m cProfile -o output.myprof [script]
#  to collect the data



import sys
import pstats

def main():
	
    results_file = sys.argv[1]
    order = 'cumulative'
    num = 100
    
    p = pstats.Stats(results_file)
    
    # Print by order
    p.sort_stats(order).print_stats(num)
    
    #~ p.sort_stats('time', 'cum').print_stats(.5, 'init')
    #~ p.print_callers(.5, 'init')
    
    return 0

if __name__ == '__main__':
	main()
