import pandas as pd
import re
import sys
import numpy as np
from ARCBackend.ARChive import ARChive
import json

if len(sys.argv) != 3:
   print("Usage: python3 hive2json.py infile.hdf5.hive outfile.json")
   exit()

infile = ".".join(sys.argv[1].split('.')[:-1])
outfile = sys.argv[2]

arc = ARChive(infile, "r")

thejson = arc.dumpJSON(filename=outfile)

arc.close()