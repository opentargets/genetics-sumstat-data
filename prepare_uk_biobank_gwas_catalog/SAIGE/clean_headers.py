#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Script to strip headers from a stream

import sys

def main():

    header_done = False
    header_str = 'chromosome'

    for line in sys.stdin:

        # Write header if not done already
        if line.startswith(header_str):
            if not header_done:
                sys.stdout.write(line)
                header_done = True
        else:
            # Convert column 2 to int
            parts = line.split('\t')
            parts[1] = str(int(parts[1].split('.')[0]))

            # Write
            sys.stdout.write('\t'.join(parts))

if __name__ == '__main__':

    main()
