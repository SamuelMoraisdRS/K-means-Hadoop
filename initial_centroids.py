#!/bin/bash/python3

import sys

k = int(sys.argv[1])

min_x, max_x = [-124.62823268, -67.172316]
min_y, max_y = [25.27794215, 49.33834794]

for i in range(k):
    print(f"{min_x + (max_x - min_x) * i / (k - 1)},{min_y + (max_y - min_y) * i / (k - 1)}")
