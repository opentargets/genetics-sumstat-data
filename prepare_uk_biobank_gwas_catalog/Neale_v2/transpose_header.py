#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy

import sys

def main():

    header = []

    # Read stdin
    for line in sys.stdin:
        header.append(line.rstrip())

    # Write as single line
    sys.stdout.write('\t'.join(header) + '\n')

if __name__ == '__main__':

    main()
