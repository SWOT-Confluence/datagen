""" Script to create sets for all algorithms, using the sets class
"""

# Standard imports
import sys
from pathlib import Path
import json

# Third-party imports
from netCDF4 import Dataset
import numpy as np

# Local imports
from sets import sets

def main():

    #context
    try:
        index_to_run=int(sys.argv[1]) #integer
    except IndexError:
        index_to_run=-235

    #data directories
    if index_to_run == -235:
        INPUT_DIR = Path("/mnt/data/input")
        OUTPUT_DIR = Path("/mnt/data/output")
    else:
        INPUT_DIR = Path("/Users/mtd/Analysis/SWOT/Discharge/Confluence/verify/InversionSets/europe/")
        OUTPUT_DIR = Path("/Users/mtd/Analysis/SWOT/Discharge/Confluence/verify/InversionSets/europe/")

    # read in file with all reaches to run
    reach_json=INPUT_DIR.joinpath('reaches.json')
    with open(reach_json) as json_file:
        reaches = json.load(json_file)

    # read in sword file
    swordfile=INPUT_DIR.joinpath('eu_sword_v11.nc')
    sword_dataset=Dataset(swordfile)

    #get set
    Algorithms=['MetroMan','HiVDI','SIC']
    
    for Algorithm in Algorithms:
      #SetData[Algorithm]={}
      print('Getting set for',Algorithm)
      params = SetParameters(Algorithm)
      print(params)

      algoset = sets(params,reaches,sword_dataset)
      InversionSets=algoset.getsets()

      # output to json file
      algoset.write_inversion_set_data(InversionSets,OUTPUT_DIR)

def SetParameters(algo):
    params={}
    params['algo']=algo
    if algo == 'MetroMan':
        params['RequireIdenticalOrbits']=True
        params['DrainageAreaPctCutoff']=10.
        params['AllowRiverJunction']=False
        params['Filename']='metrosets.json'
        params['MaximumReachesEachDirection']=2
        params['MinimumReaches']=3
    elif algo == 'HiVDI':
        params['RequireIdenticalOrbits']=False
        params['DrainageAreaPctCutoff']=30.
        params['AllowRiverJunction']=False
        params['Filename']='hivdisets.json'
        params['MaximumReachesEachDirection']=np.inf
        params['MinimumReaches']=1
    elif algo == 'SIC':
        params['RequireIdenticalOrbits']=False
        params['DrainageAreaPctCutoff']=30.
        params['AllowRiverJunction']=False
        params['Filename']='hivdisets.json'
        params['MaximumReachesEachDirection']=np.inf
        params['MinimumReaches']=1
 
    return params

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")

