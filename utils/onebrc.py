#!/usr/bin/env python3
import pandas as pd
from sys import argv

if len(argv) == 2:
    fname = argv[1]
else:
    fname = "./data/test.txt"

cities = pd.read_csv(fname, delimiter=";", names=["name", "temp"], memory_map = True, chunksize=1_000_000)
print(cities.groupby("name").apply(lambda x: (x.min(), x.mean(), x.max())))
