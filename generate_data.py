"""Script to generate: basin, reach, reach_node, cycle_pass JSON files.

Also generates a list of S3 URIs for SWOT shapefiles. Accesses PO.DAAC CMR to
generate a list.
"""

# Standard imports
import json
import os
from pathlib import Path
import re
import traceback
import zipfile

# Third-party imports
import fsspec
import shapefile
from bs4 import BeautifulSoup
import numpy as np
import fnmatch
import netCDF4
import shutil

# Local imports
from conf import conf
from datagen.Basin import Basin
from datagen.CyclePass import CyclePass
from datagen.Reach import Reach
from datagen.ReachNode import ReachNode
from datagen.S3List import S3List
from sets.getAllSets import main as set_main
import datagen.Ssc as ssc

def apply_reach_patch(sword_dataset, swordpatch):
    """Apply reach level changes two the new copy of SWORD with the suffix _patch.nc
    
    Parameters
    ----------
    sword_dataset: netCDF4 Dataset
        netCDF4 Dataset of the new copy of sword

    swordpatch: dict
        dict of changes to the sword dataset
    
    """
    all_reaches = sword_dataset['reaches']['reach_id'][:]
    for reach in list(swordpatch['reach_data'].keys()):
        if int(reach) in all_reaches:
            reach_index = np.where(all_reaches == int(reach))[0][0]
            for var in list(swordpatch['reach_data'][reach].keys()):

                if var != 'metadata':
                    if len(list(sword_dataset['reaches'][var][:])) != len(all_reaches):
                        pre_transformed_data = sword_dataset['reaches'][var][:]
                        transformed_data = pre_transformed_data.T
                        transformed_data[reach_index][:] = swordpatch['reach_data'][reach][var]
                        un_transformed_data = transformed_data.T
                        sword_dataset['reaches'][var][:] = un_transformed_data

                    
                    else:
                        sword_dataset['reaches'][var][reach_index] = swordpatch['reach_data'][reach][var]


def patch_sword(args, INPUT_DIR, sword_filename, conf):
    """Create a new copy of sword with the '_patch.nc' suffix, delete any previous patch versions
    , update the conf file with the new sword suffix, apply all patches
    """

    # create filepaths
    new_suffix = conf['sword_suffix'].replace('.nc', '_patch.nc')
    new_sword_filename = sword_filename.replace('.nc', '_patch.nc')
    swordfilepath=INPUT_DIR.joinpath("sword")
    old_swordfile=swordfilepath.joinpath(sword_filename)
    new_swordfile = swordfilepath.joinpath(new_sword_filename)

    # remove old sword_patch file, create a new one
    if os.path.exists(new_swordfile):
        os.remove(new_swordfile)

    shutil.copy(old_swordfile, new_swordfile)

    sd = netCDF4.Dataset(new_swordfile, 'a')

    # load in swordpatch
    with open(args.swordpatch) as jf:
        swordpatch = json.load(jf)

    # apply reach patch
    apply_reach_patch(sword_dataset=sd, swordpatch=swordpatch)

    return new_suffix, new_sword_filename


def extract_ids_local(shapefiledir, cont, outdir):
    """Extract reach identifiers from shapefile names and return a list.
    
    Parameters
    ----------
    shapefiledir: path
        path to local shapefiles
    cont: string
        continent abreviation
    outdir: path
        path to the directory contianing the s3 list json
    """
    
    # Extract reach identifiers from local files
    print("Extracting reach and node identifiers from shapefiles.")
    reach_ids = []
    node_ids = []
    shp_files = []
    reach_id_s3 = {}
    with os.scandir(Path(shapefiledir)) as shpfiles:
        for shpfile in shpfiles:
            if cont in shpfile.name:    # Filter by continent
                shp_files.append(shpfile.name)
                # Locate and open DBF file
                dbf_file = f"{shpfile.name.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfile, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    records = sf.records()
                    if "Reach" in shpfile.name:
                        reach_id = {rec["reach_id"] for rec in records}
                        reach_ids.extend(list(reach_id))
                        for rid in reach_id:
                            if rid in reach_id_s3.keys():
                                reach_id_s3[rid].append(shpfile)
                            else:
                                reach_id_s3[rid] = shpfile
                    if "Node" in shpfile.name:
                        node_id = {rec["node_id"] for rec in records}
                        node_ids.extend(list(node_id))
          
    # Remove duplicates from multiple files and sort
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    rids_shp = {reach_id: sorted(reach_id_s3[reach_id]) for reach_id in sorted(reach_id_s3)}
    shp_files.sort(key=sort_shapefiles)
    shp_json = [ str(Path(shapefiledir).joinpath(shp)) for shp in shp_files ]
    json_file = Path(outdir).joinpath(update_json_filename(conf["s3_list_local"], cont))
    write_json(shp_json, json_file)
    return shp_files, reach_ids, node_ids, rids_shp


def extract_s3_uris(s3_uris, s3_creds, s3_endpoint, args, cont, sword_target_version, reach_list=[], 
                    pass_list_data=[]):
    """Extract S3 URIs from reach file subset.
    
    Open shapefiles and locate reach and node identifiers.
    """
    
    reach_ids = []
    node_ids = []
    shp_files = []
    reach_id_s3 = {}
    i = 0
    for i in range(len(s3_uris)):
        
        shpfile = s3_uris[i]

        # Try to access S3 shapefiles with credentials 3 times
        retry_num = 3
        while retry_num != 0:
            print("Accessing:", shpfile, "| index =", i)            
            try:
                with fsspec.open(f"{shpfile}", mode="rb", anon=False, 
                    key=s3_creds["accessKeyId"], secret=s3_creds["secretAccessKey"], 
                    token=s3_creds["sessionToken"]) as shpfh:
                    
                    # Locate and open DBF file
                    dbf_file = f"{shpfile.split('/')[-1].split('.')[0]}.dbf"
                    
                    # Get sword version and pass number
                    zip_file = zipfile.ZipFile(shpfh, 'r')
                    correct_pass = check_sword_version_pass(dbf_file, zip_file, shpfh, shpfile, sword_target_version, pass_list_data)
                    
                    # If processing correct sword version and pass is in pass data then proceed with extracting reach and node IDs
                    if correct_pass:
                        with zip_file.open(dbf_file) as dbf:
                            sf = shapefile.Reader(dbf=dbf)
                            records = sf.records()
                            
                            # Extract REACH data
                            if "Reach" in shpfile:
                                extract_reach(records, shpfile, shp_files, reach_ids, reach_id_s3, reach_list)

                            # Extract NODE data    
                            if "Node" in shpfile:
                                extract_node(records, shpfile, shp_files, node_ids, reach_id_s3, reach_list)

                retry_num = 0
                i += 1
            except Exception as e:
                retry_num -= 1
                s3_creds = renew_credentials(s3_endpoint, args.ssmkey)
                print('generate_data - ERROR: ', e)
                print('generate_data - INFO: repulling creds and trying again, try #', retry_num)
                print('generate_data - INFO: new creds expire: ', s3_creds['expiration'])

    # Sort and remove duplicates from reaches, nodes, and shapefiles
    rid_s3 = sort_lists(reach_ids, node_ids, shp_files, reach_id_s3)
        
    return shp_files, reach_ids, node_ids, rid_s3


def check_sword_version_pass(dbf_file, zip_file, shpfh, shpfile, sword_target_version, pass_list_data=[]):
    """Check and return true if SWORD version matches target and if the
    pass number is in the pass list (if provided)."""
    
    # Check to see if we should process, we only process things from sword 15
    xml_fp = dbf_file.replace('.dbf', '.shp.xml') 
    with zip_file.open(xml_fp, 'r') as f:
        data = f.read()
    bs_data = BeautifulSoup(data, "xml")
    b_unique = bs_data.find_all('xref_prior_river_db_files')
    sword_version = str(b_unique[0]).split('>')[1].split(',')[0].split('_')[-1].split('.')[0][2:]
    pass_number = str(os.path.basename(shpfile)).split('_')[6]
    
    # Check sword version against target and pass number against possible pass list
    if sword_version == sword_target_version:
        if pass_list_data:
            if str(pass_number) in pass_list_data or int(pass_number) in pass_list_data:
                correct_pass = True
            else:
                correct_pass = False
        else:
            correct_pass = True
    else:
        correct_pass = False
    
    return correct_pass


def extract_reach(records, shpfile, shp_files, reach_ids, reach_id_s3, reach_list=[]):
    """Extract reach identifiers from shapefile."""
    
    shp_reaches = {rec["reach_id"] for rec in records}
    # Filter subset from reach list
    if reach_list:
        reach_intersection = [ value for value in shp_reaches if value in reach_list ]
        if len(reach_intersection) > 0:
            shp_files.append(shpfile)
            reach_ids.extend(reach_intersection)
            for reach_id in reach_intersection:
                track_s3_uris(reach_id_s3, reach_id, shpfile)
    # No reach list provided grab all reach identifiers
    else:
        shp_files.append(shpfile)
        reach_ids.extend(shp_reaches)
        for reach_id in shp_reaches:
            track_s3_uris(reach_id_s3, reach_id, shpfile)


def extract_node(records, shpfile, shp_files, node_ids, reach_id_s3, reach_list=[]):
    """Extract node identifiers from shapefile."""
    
    node_id = {rec["node_id"] for rec in records}
    # Filter subset from reach list
    if reach_list:
        for reach_id in reach_list:
            reach_r = re.compile(f"^{reach_id[:10]}.*")
            node_m = list(filter(reach_r.match, node_id))
            if node_m:
                node_ids.extend(node_m)
                shp_files.append(shpfile)
                track_s3_uris(reach_id_s3, reach_id, shpfile)
    # No reach list provided grab all node identifiers
    else:
        node_ids.extend(list(node_id)) 
        shp_files.append(shpfile)
        for n in node_id:
            rid = f"{n[:10]}{n[-1]}"
            track_s3_uris(reach_id_s3, rid, shpfile)


def track_s3_uris(reach_id_s3, rid, shpfile):
    """Update reach_id_s3 dictionary with shapefile URI."""
    
    if rid in reach_id_s3.keys():
        if shpfile not in reach_id_s3[rid]: reach_id_s3[rid].append(shpfile)
    else:
        reach_id_s3[rid] = [shpfile]


def renew_credentials(s3_endpoint, ssmkey):
    """Renew S3 credentials."""
    
    s3_list = S3List()
    username, password = s3_list.login()
    s3_creds = s3_list.get_s3_creds(s3_endpoint, username, password, ssmkey)
    return s3_creds


def sort_lists(reach_ids, node_ids, shp_files, reach_id_s3):
    """Sort lists to maintain order between all identifiers.
    
    Return reach identifier/shapefile dictionary.
    """
    
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    shp_files = list(set(shp_files))
    shp_files.sort(key=sort_shapefiles)
    rid_s3 = {reach_id: sorted(reach_id_s3[reach_id]) for reach_id in sorted(reach_id_s3)}
    return rid_s3


def extract_s3_uris_local(shapefiledir, cont, outdir, reach_list):
    """Extract S3 URIs from reach file subset."""
    
    print("Extracting shapefiles and node identifiers from subset.")
    # Open shapefiles and locate reach and node identifiers
    reach_ids = []
    node_ids = []
    shp_files = []
    reach_id_s3 = {}
    with os.scandir(Path(shapefiledir)) as shpfiles:
        for shpfile in shpfiles:
            if cont in shpfile.name:    # Filter by continent
                # Locate and open DBF file
                dbf_file = f"{shpfile.name.split('/')[-1].split('.')[0]}.dbf"            
                zip_file = zipfile.ZipFile(shpfile, 'r')
                with zip_file.open(dbf_file) as dbf:
                    sf = shapefile.Reader(dbf=dbf)
                    records = sf.records()
                    if "Reach" in shpfile.name:
                        shp_reaches = {rec["reach_id"] for rec in records}
                        reach_intersection = [ value for value in shp_reaches if value in reach_list ]
                        if len(reach_intersection) > 0:
                            shp_files.append(shpfile.name)
                            reach_ids.extend(reach_intersection)
                            for rid in reach_id:
                                if rid in reach_id_s3.keys():
                                    reach_id_s3[rid].append(shpfile)
                                else:
                                    reach_id_s3[rid] = shpfile
                    if "Node" in shpfile.name:
                        node_id = {rec["node_id"] for rec in records}
                        for reach_id in reach_list:
                            reach_r = re.compile(f"^{reach_id[:10]}.*")
                            node_ids.extend(list(filter(reach_r.match, node_id)))
    
    # Sort and remove duplicates
    reach_ids = list(set(reach_ids))
    reach_ids.sort()
    node_ids = list(set(node_ids))
    node_ids.sort()
    shp_files.sort(key=sort_shapefiles)
    rid_s3 = {reach_id: sorted(reach_id_s3[reach_id]) for reach_id in sorted(reach_id_s3)}
    
    # Write JSON file
    shp_json = [ str(Path(shapefiledir).joinpath(shp)) for shp in shp_files ]
    json_file = Path(outdir).joinpath(update_json_filename(conf["s3_list_local"], cont))
    write_json(shp_json, json_file)
    
    return shp_files, reach_ids, node_ids, []


def get_continent(index, json_file):
    """Retrieve continent to run datagen operations for."""
    
    i = int(index) if index != -235 else int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX"))
    with open(json_file) as jf:
        data = json.load(jf)
    return list(data[i].keys())[0].upper()


def get_subset(json_file):
    """Retrieve subset data to run datagen operations for."""
    
    with open(json_file) as jf:
        data = json.load(jf)
    return data

   
def strtoi(text):
    """Convert string digits to integers"""
    return int(text) if text.isdigit() else text


def sort_shapefiles(shapefile):
    """Sort shapefiles so that they are in ascending order."""
    
    return [ strtoi(shp) for shp in re.split(r'(\d+)', shapefile) ]


def write_json(json_object, filename):
    """Write JSON object as a JSON file to the specified filename."""

    with open(filename, 'w') as jf:
        json.dump(json_object, jf, indent=2)


def run_aws(args, cont, sword_target_version,reach_list=False, pass_list_data=False):
    """Executes operations to retrieve reach identifiers from shapefiles hosted
    in AWS S3 bucket."""

    # Retrieve a list of S3 files
    print(f"Retrieving and storing list of S3 URIs for {cont}.")
    s3_list = S3List()
    try:
        if args.simulated:
            s3_uris, s3_creds = s3_list.get_s3_uris_sim()
        else:
            s3_endpoint = conf["s3_cred_endpoints"][args.provider]
            s3_uris, s3_creds = s3_list.login_and_run_query(args.shortname, args.provider, args.temporalrange, cont, s3_endpoint, args.ssmkey)
            s3_uris.sort(key=sort_shapefiles)
            print('here are some sample urls that are sorted...', s3_uris[:1])
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        print("Error encountered. Exiting program.")
        exit(1)
    
    if s3_uris:
        s3_uris, reach_ids, node_ids, rid_s3 = extract_s3_uris(s3_uris=s3_uris, 
                                                               s3_creds=s3_creds, 
                                                               s3_endpoint=s3_endpoint,
                                                               args=args,
                                                               reach_list=reach_list,
                                                               sword_target_version = sword_target_version,
                                                               pass_list_data=pass_list_data,
                                                               cont = cont)
        print('Here are some extracted s3_uris...', s3_uris[:1])
        if reach_ids:    
            # Write shapefile json
            json_file = Path(args.directory).joinpath(update_json_filename(conf["s3_list"], cont))
            write_json(s3_uris, json_file)
            
            # Write reach id S3 json
            json_file = Path(args.directory).joinpath(f"s3_reach_{cont.lower()}.json")
            write_json(rid_s3, json_file)
            
            # Creat a list of only shapfile names
            shp_files = [shp.split('/')[-1].split('.')[0] for shp in s3_uris]
            return shp_files, reach_ids, node_ids
        else:
            print('No reach ids found...')
            return [], [], []
    else:
        return [], [] ,[]


def update_json_filename(json_file, continent):
    """Update JSON file name to include continent."""
    
    filename_pieces = json_file.split('.')
    cont_name = f"{filename_pieces[0]}_{continent.lower()}.{filename_pieces[1]}"
    return cont_name


def run_local(args, cont, subset, reach_list=None):
    """Load shapefiles in from local file system and return reach identifiers."""
    
    # Extract reach identifiers
    if subset == False:
        shp_files, reach_ids, node_ids, rids_shp = extract_ids_local(args.shapefiledir, cont, args.directory)
    
    # Extract shapefiles and node identifiers for reach identifier subset
    else:
        shp_files, reach_ids, node_ids, rids_shp = extract_s3_uris_local(args.shapefiledir, cont, args.directory, reach_list)
        
    if rids_shp:
        json_file = Path(args.directory).joinpath(f"s3_reach_{cont.lower()}.json")
        write_json(rids_shp, json_file)
    
    return shp_files, reach_ids, node_ids


def run_river(args):
    """Execute the operations needed to generate JSON data."""

    INPUT_DIR = Path(args.directory)
    
    # Determine continent to run on
    cont = get_continent(args.index, Path(args.directory).joinpath(args.jsonfile))
    SWORD_version = args.swordversion
    
    # Determine if global or subset run
    if args.subsetfile:
        reach_list = get_subset(args.subsetfile)
        reach_list = [str(i) for i in reach_list]
        subset = True
    else:
        reach_list = []
        subset = False
    
    if args.passlist:
        with open(args.passlist) as jf:
            pass_list_data = json.load(jf)
    else:
        pass_list_data = False
    
    # Determine where run is taking place (local or aws)
    if args.local:
        shp_files, reach_ids, node_ids = run_local(args, cont, subset, reach_list)
    else:
        shp_files, reach_ids, node_ids = run_aws(args=args, cont=cont, reach_list=reach_list, sword_target_version = SWORD_version,pass_list_data=pass_list_data)
    
    if shp_files:
        # Create cycle pass data
        cycle_pass = CyclePass(shp_files)
        cycle_pass_data, pass_num = cycle_pass.get_cycle_pass_data()
        json_file = Path(args.directory).joinpath(update_json_filename(conf["cycle_passes"], cont))
        print(f"Writing cycle pass data to: {json_file}")
        write_json(cycle_pass_data, json_file)
        json_file = Path(args.directory).joinpath(update_json_filename(conf["passes"], cont))
        print(f"Writing pass number data to: {json_file}")
        write_json(pass_num, json_file)
        
        # Filenames
        sword_filename = f"{cont.lower()}_{conf['sword_suffix']}"
        sos_filename = f"{cont.lower()}_{conf['sos_suffix']}"

        # Patch SWORD Issues
        if args.swordpatch:
            print('Patching SWORD')
            conf['sword_suffix'], sword_filename = patch_sword(args, INPUT_DIR, sword_filename, conf)
            print('Finished patching, new suffix and filename:', conf['sword_suffix'], sword_filename)

        # Create basin data
        print("Retrieving basin data.")
        basin = Basin(reach_ids, sword_filename, sos_filename)
        basin_data = basin.extract_data()
        json_file = Path(args.directory).joinpath(update_json_filename(conf["basin"], cont))
        print(f"Writing basin data to: {json_file}")
        write_json(basin_data, json_file)
        
        # Create reach data
        print("Retrieving reach data.")
        reach = Reach(reach_ids, sword_filename, sos_filename)
        reach_data = reach.extract_data()
        json_file = Path(args.directory).joinpath(update_json_filename(conf["reach"], cont))
        print(f"Writing reach data to: {json_file}")
        write_json(reach_data, json_file)
        
        # Create reach node data
        print("Retrieving reach node data.")
        reach_node = ReachNode(reach_ids, node_ids)
        reach_node_data = reach_node.extract_data()
        json_file = Path(args.directory).joinpath(update_json_filename(conf["reach_node"], cont))
        print(f"Writing reach node data to: {json_file}")
        write_json(reach_node_data, json_file)   
        
        # Create sets 
        print("Retrieving set data.")
        set_main(args, cont, INPUT_DIR, INPUT_DIR)

        # Create ssc mapping
        if args.hls:
            print("Retrieving HLS tiles.")
            swordfilepath = os.path.join(INPUT_DIR,'sword', sword_filename)
            json_file = Path(args.directory).joinpath(update_json_filename(conf["hls_links"], cont))
            hls_link_data = ssc.ssc_process_continent(reach_ids, cont, swordfilepath, args.temporalrange)
            write_json(hls_link_data, json_file)
    
    else:
        print("No shapefiles were located and therefore no JSON files will be written.")


if __name__ == "__main__":
    import datetime
    start = datetime.datetime.now()
    run_river()
    end = datetime.datetime.now()
    print(f"Execution time: {end - start}")