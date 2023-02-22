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
try:
    from sets import Sets
except ImportError:
    from sets.sets import Sets

def main(args=None, continent=None):

    #context
    if len(sys.argv) <= 2:
        try:
            index_to_run=int(sys.argv[1]) #integer
        except IndexError:
            index_to_run=-235
            
        #other commandline arguments
        continent=sys.argv[2]
    else:
        index_to_run=args.index
        continent=continent

    #data directories
    if index_to_run == -235:
        INPUT_DIR = Path("/mnt/data")
        OUTPUT_DIR = Path("/mnt/data")
    else:
        INPUT_DIR = Path("/Users/mtd/Analysis/SWOT/Discharge/Confluence/verify/InversionSets/europe/")
        OUTPUT_DIR = Path("/Users/mtd/Analysis/SWOT/Discharge/Confluence/verify/InversionSets/europe/")

    # read in file with all reaches to run
    reach_json=INPUT_DIR.joinpath(f"reaches_{continent.lower()}.json")
    with open(reach_json) as json_file:
        reaches = json.load(json_file)

    # figure out which sword file to read
    swordfile=reaches[0]['sword']

    # read in sword file
    swordfilepath=INPUT_DIR.joinpath(swordfile) if index_to_run != -235 else INPUT_DIR.joinpath("sword", swordfile)
    sword_dataset=Dataset(swordfilepath)

    #get set
    Algorithms=['MetroMan','HiVDI','SIC']
    # Algorithms=['HiVDI']
    # Algorithms=['MetroMan','HiVDI']
    
    for Algorithm in Algorithms:
        print('Getting set for',Algorithm)
        params = SetParameters(Algorithm, continent)
        print(params)

        algoset = Sets(params,reaches,sword_dataset)
        InversionSets=algoset.getsets()

        # output to json file
        algoset.write_inversion_set_data(InversionSets,OUTPUT_DIR)

    #close sword dataset
    sword_dataset.close()  

def SetParameters(algo, cont):
    params={}
    params['algo']=algo
    if algo == 'MetroMan':
        params['RequireIdenticalOrbits']=True
        params['DrainageAreaPctCutoff']=10.
        params['AllowRiverJunction']=False
        params['Filename']=f'metrosets_{cont.lower()}.json'
        params['MaximumReachesEachDirection']=2
        params['MinimumReaches']=3
        params['AllowedReachOverlap']=-1 # specify -1 to just remove duplicates
        # params['']
    elif algo == 'HiVDI':
        params['RequireIdenticalOrbits']=False
        params['DrainageAreaPctCutoff']=30.
        params['AllowRiverJunction']=False
        params['Filename']=f'hivdisets_{cont.lower()}.json'
        params['MaximumReachesEachDirection']=np.inf
        params['MinimumReaches']=1
        params['AllowedReachOverlap']=.5
    elif algo == 'SIC':
        params['RequireIdenticalOrbits']=False
        params['DrainageAreaPctCutoff']=30.
        params['AllowRiverJunction']=False
        params['Filename']=f'sicsets_{cont.lower()}.json'
        params['MaximumReachesEachDirection']=np.inf
        params['MinimumReaches']=1
        params['AllowedReachOverlap']=.67
 
    return params

if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    end = datetime.now()
    print(f"Execution time: {end - start}")

