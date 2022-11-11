class Reach:
    """
    A class that maps reach identifiers to SWOT reach, SWOT node, and SoS data 
    files. 
    
    Both reach identifiers are reach sets are created. The output is written as 
    JSON data to a file.

    Attributes
    ----------
    reach_ids: list
        list of reach identifiers
    reach_data: list
        list of dictionaries of reach id keys and filename values
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
        reach_ids: List
            List of reach identifierse
        """
        
        self.reach_ids = reach_ids
        self.reach_data = []
        self.sos_filename = sos_filename
        self.sword_filename = sword_filename

    def extract_data(self):
        """Extracts reach identifier from and maps to SWOT, SoS, SWORD files.
        
        Populates reach_data attribute.
        """
        
        # Extract reach data
        for reach_id in self.reach_ids:
            self.reach_data.append({
                "reach_id": int(reach_id), 
                "sword": self.sword_filename,
                "swot": f"{reach_id}_SWOT.nc",
                "sos": self.sos_filename
            })
        return self.reach_data