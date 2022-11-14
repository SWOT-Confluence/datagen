class CyclePass:
    """A class that map cycle pass combinations to numeric idenitifiers.
    
    The class can also translate numeric identifiers back to cycle pass 
    combinations.
    
    Attributes
    ----------
    shp_files: list
        list of shapefiles
        
    Methods
    -------
    """
    
    def __init__(self, shp_files):
        """
        shp_files: list
            list of shapefiles
        """
        
        self.shp_files = shp_files
        self.cycle_pass_data = {}
        self.pass_num = {}
        
    def get_cycle_pass_data(self):
        """Return cycle pass combinations associated with numeric identifier."""
        
        p = 1
        for shp_file in self.shp_files:
            cycle_no = shp_file.split('_')[5]
            pass_no = shp_file.split('_')[6]
            if not f"{cycle_no}_{pass_no}" in self.cycle_pass_data:
                self.cycle_pass_data[f"{cycle_no}_{pass_no}"] = p
                self.pass_num[p] = [cycle_no, pass_no]
            p += 1
        return self.cycle_pass_data, self.pass_num