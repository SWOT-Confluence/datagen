""" Script to create sets for all algorithms, using the sets class
"""

# Standard imports
import os
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

    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            print('Running from interatctive shell (e.g. Jupyter notebook) detected. Modifying command line args')
            sys.argv=sys.argv[1:]
            #print(len(sys.argv))
            #print(sys.argv)
    except NameError:
        print("Not running in Jupyter notebook.")

    if len(sys.argv) <= 2:
        try:
            index_to_run=int(sys.argv[0]) #integer
        except IndexError:
            index_to_run=-235
            
        continent=sys.argv[1]
    else:
        index_to_run=int(args.index)
        continent=continent
        
    ## temporary change - for mike's debug march 6
    #index_to_run=sys.argv[1]
    #continent=sys.argv[2]
    

    #data directories
    #if index_to_run == -235 or len(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")) > 0:
    if index_to_run == -235 or type(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")) != type(None):
        INPUT_DIR = Path("/data")
        OUTPUT_DIR = Path("/data")
        swordfilepath=INPUT_DIR.joinpath("sword")
    else:
        INPUT_DIR = Path("/Users/mtd/Analysis/SWOT/Discharge/Confluence/verify/InversionSets/europe/")
        OUTPUT_DIR = Path("/Users/mtd/Analysis/SWOT/Discharge/Confluence/verify/InversionSets/europe/")
        swordfilepath=INPUT_DIR

    # read in file with all reaches to run
    reach_json=INPUT_DIR.joinpath(f"reaches_{continent.lower()}.json")
    with open(reach_json) as json_file:
        reaches = json.load(json_file)
        
    # figure out which sword file to read
    swordfile=swordfilepath.joinpath(reaches[0]['sword'])

    # read in sword file
    sword_dataset=Dataset(swordfile)

    #get set
    Algorithms=['MetroMan','HiVDI','SIC']
    #Algorithms=['MetroMan']
    
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

