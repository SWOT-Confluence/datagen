# Standard imports
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
    sos_filename: str
        name of SOS file
    sword_filename: str
        name of SWORD file 
    
    Methods
    -------
    extract_data()
        extracts reach identifier and maps to file name
    """

    def __init__(self, reach_ids, sword_filename, sos_filename):
        """
        Parameters
        ----------
        output_dir: Path
            path to write JSON output file to
        """
        
        self.basin_data = []
        self.reach_ids = reach_ids
        self.sos_filename = sos_filename
        self.sword_filename = sword_filename

    def extract_data(self):
        """Extracts basin and reach identifier and maps to data file names.
        
        Populates basin_data attribute.

        TODO:
        - Do basins cross continents? As the basin identifier is shared across
        continents in SWORD.
        """
        
        # Retrieve basin identifiers, reach identifiers, SWORD file names, and SOS file names
        self.get_sword()
                
        # Assign SWOT file names
        self.get_swot()
        
        return self.basin_data
        
    def get_sword(self):
        """Associate basin identifiers, reach identifiers and SWORD file names."""
        
        basin_ids = set(list(map(lambda x: int(str(x)[0:4]), self.reach_ids)))
        for basin_id in basin_ids:
            self.basin_data.append({"basin_id": basin_id, 
                                    "reach_id": extract_reach_ids(basin_id, self.reach_ids),
                                    "sword": self.sword_filename,
                                    "sos": self.sos_filename})
            
    def get_swot(self):
        """Assign SWOT file names to basin_data dictionaries."""
        
        for element in self.basin_data:
            element["swot"] = []
            for reach_id in element["reach_id"]:
                element["swot"].append(f"{reach_id}_SWOT.nc")

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