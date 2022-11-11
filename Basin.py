# Standard imports
from datetime import date
import json
from pathlib import Path
import re

class Basin:
    """
    A class that maps basin identifiers to SWORD and SoS data 
    files. 
    
    The output is written as JSON data to a file.

    Attributes
    ----------
    basin_data: list
        list of dictionaries of basin identifier keys and filename values
    reach_ids: list
        list of string reach identifiers
    SOS_SUFFIX: str
        suffix to append to the SWORD file name
    SWORD_FILENAME: str
        name of SWORD file that holds Ohio River data 
    
    Methods
    -------
    extract_data()
        extracts reach identifier and maps to file name
    """
    
    SWORD_FILENAME = "na_sword_v11.nc"
    SOS_SUFFIX = "SOS_priors.nc"

    def __init__(self, reach_ids):
        """
        Parameters
        ----------
        output_dir: Path
            path to write JSON output file to
        """

        self.basin_data = []
        self.reach_ids = reach_ids

    def extract_data(self):
        """Extracts basin and reach identifier and maps to data file names.
        
        Populates basin_data attribute.

        TODO:
        - Do basins cross continents? As the basin identifier is shared across
        continents in SWORD.
        """
        
        # Retrieve basin identifiers, reach identifiers, and SWORD file names
        self.get_sword()
                
        # Assign SWOT file names
        self.get_swot()
        
        # Assign SOS file names
        self.get_sos()
        
        return self.basin_data
        
    def get_sword(self):
        """Associate basin identifiers, reach identifiers and SWORD file names."""
        
        basin_ids = set(list(map(lambda x: int(str(x)[0:6]), self.reach_ids)))
        for basin_id in basin_ids:
            self.basin_data.append({"basin_id": basin_id, 
                                    "reach_id": extract_reach_ids(basin_id, self.reach_ids),
                                    "sword": self.SWORD_FILENAME})
            
    def get_swot(self):
        """Assign SWOT file names to basin_data dictionaries."""
        
        for element in self.basin_data:
            element["swot"] = []
            for reach_id in element["reach_id"]:
                element["swot"].append(f"{reach_id}_SWOT.nc")
                
    def get_sos(self):
        """Assign SOS file names to basin_data dictionaries."""
        
        for element in self.basin_data:
            element["sos"] = f"{element['sword'].split('.')[0]}_SOS.nc"

def extract_reach_ids(basin_id, reach_ids):
    """Extract all reach identfiers for basin.

    Parameters
    ----------
    basin_id: int
        basin identifier
    reach_ids: numpy.ndarray
        numpy array of integer reach identifiers
    """

    basin_r = re.compile(f"^{basin_id}.*")
    return(list(filter(basin_r.match, reach_ids)))