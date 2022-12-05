# Standard imports
import re

class ReachNode:
    """
    A class that maps reach identifiers to node identifiers.
    
    The output is written as JSON data to a file.

    Attributes
    ----------
    node_ids: list
        list of node identifiers
    reach_ids: list
        list of reach identifiers
    reach_node_data: dict
        dictionary of data to be written to json file
    
    Methods
    -------
    extract_data()
        extracts node identifiers and maps to reach identifiers
    """

    def __init__(self, reach_ids, node_ids):
        """
        Parameters
        ----------
        json_file: Path
            path to JSON file to write JSON data to
        sword_dir: Path
            path to SWORD directory containing NetCDF files
        """

        self.node_ids = node_ids
        self.reach_ids = reach_ids
        self.reach_node_data = []

    def extract_data(self):
        """Extracts reach and node identifiers from SWORD.
        
        Populates json_data attribute.
        """
        
        for reach_id in self.reach_ids:
            reach_r = re.compile(f"^{reach_id[:10]}.*")
            nodes = list(filter(reach_r.match, self.node_ids))
            self.reach_node_data.append([reach_id, nodes])
        
        return self.reach_node_data