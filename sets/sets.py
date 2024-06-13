# Standard imports
import itertools
import json
import sys

# Third-party imports
import numpy as np
# import matplotlib.pyplot as plt

#maps
import folium
from folium.plugins import BeautifyIcon as BI
import pandas as pd
import random
import webbrowser

class Sets:
    """ Divide a list of reaches into inversion sets.

    Attributes
    ----------
    params: dict
        dictionary of parameters to control how sets get defined   

    
    Methods
    -------

    """
    def __init__(self,params,reaches,sword_dataset):

        self.params=params
        self.reaches=reaches
        self.sword_dataset=sword_dataset

    def extract_data_sword_continent_file(self):
        """
        Extracting data that is used to defind sets from SWORD
        """
        swordreachids=self.sword_dataset["reaches/reach_id"][:]
        sword_data_continent={}

        # grab sizes of the data
        dimfields=['orbits','num_domains','num_reaches']
        for field in dimfields:
            sword_data_continent[field]=self.sword_dataset['reaches'].dimensions[field].size

        # grab data    
        reachfields=['reach_id','facc','n_rch_up','n_rch_down','rch_id_up','rch_id_dn','swot_obs','swot_orbits']
        for field in reachfields:
            sword_data_continent[field]=self.sword_dataset['reaches/' + field][:]

        return swordreachids,sword_data_continent

    def extract_inversion_sets_by_reach(self,sword_data_continent,swordreachids):
        """
        loop over all reaches and create a set for each
        """

        InversionSets={}
        nreach=len(self.reaches)
        count=0
        for reach in self.reaches:
            count+=1  
            #print('finding set for reach',reach['reach_id'])
            #if count % 100 == 0:
            #    print('Processing reach ',count,'/',nreach)
            k=np.argwhere(swordreachids == reach['reach_id'])
            try:
                k=k[0,0] # not sure why argwhere is returning this as a 2-d array. this seems inelegant
                # print(k)
            except:
                continue
        
            sword_data_reach=self.pull_sword_attributes_for_reach(sword_data_continent,k)

            if sword_data_reach['n_rch_up']==1:
                InversionSet=self.find_set_for_reach(sword_data_reach,swordreachids,sword_data_continent)
                InversionSet['ReachList'],InversionSet['numReaches']=self.get_reach_list(InversionSet)
                InversionSets[reach['reach_id']]=InversionSet
            

        return InversionSets

    def pull_sword_attributes_for_reach(self,sword_data_continent,k):
        """
        Pull out needed SWORD data from the continent dataset arrays for a particular reach    
        """

        sword_data_reach={}
        # extract all single-dimension variables, including number of orbits and reach ids needed for multi-dim vars
        for key in sword_data_continent:
            if np.shape(sword_data_continent[key]) == (sword_data_continent['num_reaches'],):
                sword_data_reach[key]=sword_data_continent[key][k]

        # extract multi-dim vars
        for key in sword_data_continent:
            if key == 'rch_id_up':
                sword_data_reach[key]=sword_data_continent[key][0:sword_data_reach['n_rch_up'],k]
            elif key == 'rch_id_dn':
                sword_data_reach[key]=sword_data_continent[key][0:sword_data_reach['n_rch_down'],k]
            elif key == 'swot_orbits':
                sword_data_reach[key]=sword_data_continent[key][0:sword_data_reach['swot_obs'],k]

        return sword_data_reach

    def find_set_for_reach(self,sword_data_reach,swordreachids,sword_data_continent):    
        """Seting parameters for setfinder

        Parameters
        ----------
        sword_data_reach: dict
            Dictionary of relevant data from SWORD
        swordreachids: list
            List of sword reach ids
        """
        
        # ok so lets define a set:



        CheckVerbosity=False

        # 1. initialize
        InversionSet={}
        InversionSet['OriginReach']=sword_data_reach
        InversionSet['Reaches']={}
        InversionSet['Reaches'][sword_data_reach['reach_id']]=sword_data_reach
        # initially, the upstream and downstream reaches are both set to the origin reach
        InversionSet['UpstreamReach']=sword_data_reach
        InversionSet['DownstreamReach']=sword_data_reach
        # 2. check whether we can expand upstream. keep going upstream until we hit an invalid reach
        UpstreamReachIsValid=True
        n_up_add=0
        while UpstreamReachIsValid:
            upstream_reaches = InversionSet['UpstreamReach']['rch_id_up']
            upstream_reaches = upstream_reaches.data
            kup=np.argwhere(swordreachids == upstream_reaches)

            if len(kup)!=1:
                  UpstreamReachIsValid=False
            else:
                  kup=kup[0,0]
                  sword_data_reach_up=self.pull_sword_attributes_for_reach(sword_data_continent,kup)
                  UpstreamReachIsValid=self.CheckReaches(sword_data_reach,sword_data_reach_up,'up',CheckVerbosity)

            if UpstreamReachIsValid:
                #its valid, add a new reach to the set
                InversionSet['Reaches'][sword_data_reach_up['reach_id']]=sword_data_reach_up
                InversionSet['UpstreamReach']=sword_data_reach_up
                n_up_add+=1
                if n_up_add > self.params['MaximumReachesEachDirection']:
                    UpstreamReachIsValid=False
                    
        #print('added',n_up_add,'reaches in upstream direction')
                    

        # 3. check whether we can expand downstream. keep going downstream until we hit an invalid reach
        DownstreamReachIsValid=True
        n_dn_add=0
        while DownstreamReachIsValid:
            kdn=np.argwhere(swordreachids == InversionSet['DownstreamReach']['rch_id_dn'])

            if len(kdn)!=1:
                DownstreamReachIsValid=False
            else:
                kdn=kdn[0,0]
                sword_data_reach_dn=self.pull_sword_attributes_for_reach(sword_data_continent,kdn)
                
                # print('checking...',sword_data_reach_dn['reach_id'])
                
                DownstreamReachIsValid=self.CheckReaches(sword_data_reach,sword_data_reach_dn,'down',CheckVerbosity)
                if DownstreamReachIsValid:
                    InversionSet['Reaches'][sword_data_reach_dn['reach_id']]=sword_data_reach_dn
                    InversionSet['DownstreamReach']=sword_data_reach_dn
                    n_dn_add+=1
                    if n_dn_add > self.params['MaximumReachesEachDirection']:
                        DownstreamReachIsValid=False
                        
        #print('added',n_dn_add,'reaches in downstream direction')
        
        return InversionSet

    def CheckReaches(self,sword_data_reach,sword_data_reach_adjacent,direction,verbose):

        reach_ids=[]
        for reach in self.reaches:
             reach_ids.append(reach['reach_id'])

        AdjacentReachInReaches=sword_data_reach_adjacent['reach_id'] in reach_ids
        
        AdjacentReachIsRiver=str(sword_data_reach_adjacent['reach_id'])[-1]=='1'

        OrbitsAreIdentical=False
        if sword_data_reach['swot_obs']==sword_data_reach_adjacent['swot_obs']:
            OrbitsAreIdentical=list(sword_data_reach['swot_orbits'])==list(sword_data_reach_adjacent['swot_orbits'])
        AccumulationAreaDifferencePct=(sword_data_reach_adjacent['facc']-sword_data_reach['facc'])/sword_data_reach['facc']*100

        RiverJunctionPresent=False
        if sword_data_reach['n_rch_up'] > 1 or sword_data_reach['n_rch_down']>1  \
             or sword_data_reach_adjacent['n_rch_up'] > 1 or sword_data_reach_adjacent['n_rch_down']>1:
            RiverJunctionPresent=True

        ReachesMakeAValidSet=True
        if self.params['RequireIdenticalOrbits'] and not OrbitsAreIdentical:
            ReachesMakeAValidSet=False
        if AccumulationAreaDifferencePct > self.params['DrainageAreaPctCutoff']:
            ReachesMakeAValidSet=False
        if not self.params['AllowRiverJunction'] and RiverJunctionPresent:
            ReachesMakeAValidSet=False
        if self.params['RequireSetReachesInput'] and not AdjacentReachInReaches:
            ReachesMakeAValidSet=False
        if not AdjacentReachIsRiver:
            ReachesMakeAValidSet=False
     
        if verbose:
            print('reach:',sword_data_reach)
            print('adjacent reach is in reach file:',sword_data_reach_adjacent)
            print('adjacent reach is a river:',sword_data_reach_adjacent)
            print('drainage area pct diff:',AccumulationAreaDifferencePct)
            print('same swot coverage as adjacent reach',OrbitsAreIdentical)
            print('there is a river junction present:',RiverJunctionPresent)
            print('These reaches are a valid set:',ReachesMakeAValidSet)

        return ReachesMakeAValidSet

    def get_reach_list(self,InversionSet):

        if len(InversionSet['Reaches'].keys()) == 1:
            #then the reach list is just one reach
            ReachList=[InversionSet['OriginReach']['reach_id']]
            numReaches=1
        else:
            # make a list of the reaches in the set, in order from upstream to downstream
            ReachList=[]
            ReachList.append(InversionSet['UpstreamReach']['reach_id'])
            EndOfSetReached=ReachList[-1]==InversionSet['DownstreamReach']

            # sort the list of reach ids
            while not EndOfSetReached:
                CurrentEndOfSet=ReachList[-1]
                try: 
                    #can reach here when there are SWORD topological inconsistencies
                    next_reach_id_downstream=InversionSet['Reaches'][CurrentEndOfSet]['rch_id_dn'][0]
                except:
                    EndOfSetReached=True                     

                if not EndOfSetReached:
                    ReachList.append(next_reach_id_downstream)
                    EndOfSetReached=ReachList[-1]==InversionSet['DownstreamReach']['reach_id']

            numReaches=len(ReachList)
        return ReachList,numReaches

    def remove_duplicate_or_high_overlap_sets(self,InversionSets,swordreachids,sword_data_continent):
        print('removing dupes...')
        #1 switch inversion set datatype from dict to list
        InversionSetsList=self.get_IS_list(InversionSets,'','','remove_dupes') # this returns a list of dictionaries of reaches

        # idx=3
        # print('in `remove_duplicate...`. Inversion Set for index=',idx,InversionSetsList[idx])
        # sys.exit()
        
        # 2 remove duplicate and high overlap sets
        InversionSetsListNoDupes=[]
        for InversionSet in InversionSetsList:
            SetAlreadyIncluded_or_HighOverlap=False
            for ISnew in InversionSetsListNoDupes:
                if self.CheckSetsAreSame_or_HighOverlap(InversionSet,ISnew):
                    SetAlreadyIncluded_or_HighOverlap=True
            if not SetAlreadyIncluded_or_HighOverlap:
                InversionSetsListNoDupes.append(InversionSet)               
       
        # print('before removing dupes and high overlap, n=',len(InversionSetsList))
        # print('after removing dupes and high overlap, n=',len(InversionSetsListNoDupes))
        
        # print('First Inversion Set:',InversionSetsListNoDupes[0])
        
        # sys.exit()

        # 3 switch inversion set data type back to dictionary
        # transforms from list of dictionaries back to dictionary of dictionaries
        #  ... now, though, the keys are different. 
        InversionSetsNoDupes={}
        for InversionSet in InversionSetsListNoDupes:
            ReachList=self.MakeReachList(InversionSet)
            
            setkey=InversionSet[0]['origin']
            
            InversionSetsNoDupes[setkey]={}
            InversionSetsNoDupes[setkey]['ReachList']=ReachList
            InversionSetsNoDupes[setkey]['numReaches']=len(InversionSet)
            InversionSetsNoDupes[setkey]['Reaches']={}

            for reachid in InversionSetsNoDupes[setkey]['ReachList']:
                k=np.argwhere(swordreachids == np.int64(reachid))
                try:
                    k=k[0,0] # not sure why argwhere is returning this as a 2-d array. this seems inelegant
                except:
                    continue
                sword_data_reach=self.pull_sword_attributes_for_reach(sword_data_continent,k)
                InversionSetsNoDupes[setkey]['Reaches'][reachid]=sword_data_reach

        return InversionSetsNoDupes

    #function to check for duplicates
    def CheckSetsAreSame_or_HighOverlap(self,Set1,Set2):
        
        Set1List=self.MakeReachList(Set1)
        Set2List=self.MakeReachList(Set2)               
        
        SetsAreSame=False
        if len(Set1)==len(Set2):
        
            if Set1List==Set2List:
                SetsAreSame=True

        if not SetsAreSame:

            # if they are not the same, test for overlap
            overlap = list( set(Set1List) & set(Set2List))
            pctoverlap= len(overlap) / ( (len(Set1List) + len(Set2List))/2 )

            #if pctoverlap > 0.67:
            if self.params['AllowedReachOverlap'] != -1 and pctoverlap > self.params['AllowedReachOverlap']:
                #print('removing set with overlap=',pctoverlap,'for',combo[1])
                SetsAreSame = True
        
        return SetsAreSame

    # function to make a reach list
    def MakeReachList(self,Set):
        ReachList=[]
        for Reach in Set:
            ReachList.append(Reach['reach_id'])
        
        #sort does ascending by default. reverse goes descending, from upstream to downstream in SWORD
        ReachList.sort(reverse=True) 

        
        return ReachList
 
    def remove_high_overlap_sets(self,InversionSets):
        HighOverlap=True
        itercount=0

        #maxiter of 10 or 100 to debug. 10 takes 45 sec. 100 takes 3.5 minutes. 1e4 needed to generate good sets
        #maxiter=10
        maxiter=1e4

        print('... remove_high_overlap_sets, starting with ',len(InversionSets),' sets')

        while HighOverlap:
            itercount+=1
            if itercount % 10 == 0:
                print('.... iteration #',itercount, '; maximum is ', maxiter, ' iterations.')

            reaches=list(InversionSets.keys())
            reach_combos=list(itertools.combinations(reaches,2))

            ncombos=len(reach_combos)
            #print('there are ',ncombos,'combinations')
 
            nremoved=0
            for combo in reach_combos:
                is0=InversionSets[combo[0]]['ReachList']
                is1=InversionSets[combo[1]]['ReachList']
                overlap = list( set(is0) & set(is1))
                pctoverlap= len(overlap) / ( (len(is0) + len(is1))/2 )
 
                #if pctoverlap > 0.67:
                if pctoverlap > self.params['AllowedReachOverlap']:
                    #print('removing set with overlap=',pctoverlap,'for',combo[1])
                    nremoved+=1
                    del InversionSets[combo[1]]
                    break
                if combo==reach_combos[-1]:
                    HighOverlap=False

            if itercount>maxiter or nremoved == 0:
                HighOverlap=False

        return InversionSets
 
    def add_single_reach_sets(self,InversionSets,swordreachids,sword_data_continent):
        #only add reaches that are type 

        #get all reaches currently in sets
        reaches_in_sets=[]
        for IS in InversionSets:
            reach_list=InversionSets[IS]['ReachList']
            for reach in reach_list:
                reaches_in_sets.append(int(reach))
 
        # get all reaches 
        all_reaches=[]
        for reach in self.reaches:
             all_reaches.append(int(reach['reach_id']))
             #reachid=reach['reach_id']
             #reachidstr=str(reachid)
             #if reachidstr[:-1]=='1':
                 #all_reaches.append(int(reachid))
                 #all_reaches.append(int(reach['reach_id']))

        #get a list of reaches that are in all_reaches, but NOT in reaches_in_sets
        excluded_reaches = list(set(all_reaches) - set(reaches_in_sets))


        #add all "excluded" reaches to InversionSets
        iadd=0
        for excluded_reach in excluded_reaches:
            #print('adding ',excluded_reach)
            excluded_reach_str=str(excluded_reach)
            if excluded_reach_str[-1]=='1':
                iadd+=1
                InversionSet={}
                InversionSet['ReachList']=[excluded_reach]
                InversionSet['numReaches']=1
                InversionSet['Reaches']={}

                k=np.argwhere(swordreachids == np.int64(excluded_reach))
                try:
                    k=k[0,0] # not sure why argwhere is returning this as a 2-d array. this seems inelegant
                except:
                    continue
                sword_data_reach=self.pull_sword_attributes_for_reach(sword_data_continent,k)

                InversionSet['Reaches'][excluded_reach]=sword_data_reach

                #print(InversionSet)
                InversionSets[excluded_reach]=InversionSet

        print('added in ', iadd, ' single-set reaches')

        return InversionSets


    def remove_sets_with_non_river_reaches(self,InversionSets):

       nsets=len(InversionSets)
       SetIsBad={}

       iscount=0

       for IS in InversionSets:
            ContainsNonRiverReach=False
            for reach in InversionSets[IS]['Reaches']:
                reachstr=str(reach)
                reachtype=reachstr[-1]
                if reachtype != '1':
                    ContainsNonRiverReach=True
            if ContainsNonRiverReach:
                SetIsBad[IS]=True
            else:
                SetIsBad[IS]=False

       for IS in SetIsBad:
            if SetIsBad[IS]:
                del InversionSets[IS]

       return InversionSets

    def remove_small_sets(self,InversionSets):

       reaches=list(InversionSets.keys())

       # first simply remove any reach that has fewer than the minimum
       for reach in reaches:
           if InversionSets[reach]['numReaches'] < self.params['MinimumReaches']:
               del InversionSets[reach]

       # second, if it's a one-reach-set, remove if the reach exists in another set
       reaches=list(InversionSets.keys())
       SetsToRemove=[]
       for reach in reaches:
          if InversionSets[reach]['numReaches'] == 1:
              for otherreach in reaches:
                  if otherreach != reach and reach in InversionSets[otherreach]['ReachList']: 
                      SetsToRemove.append(reach)

       # remove duplicates
       SetsToRemove=list(set(SetsToRemove))

       for reach in SetsToRemove:
          del InversionSets[reach] 

       return InversionSets

    def MKmap(self,IS):
        #firt pull centerline XY from SWORD based on IS
        CL_RID=self.sword_dataset['nodes/reach_id'][:].filled(np.nan)
        px=self.sword_dataset['nodes/x'][:].filled(np.nan)
        py=self.sword_dataset['nodes/y'][:].filled(np.nan)
 
        df=pd.DataFrame(columns=["ID", "x",'y'])
        for key in IS:
            TS=IS[key]
            RIDS=TS['ReachList']
            IS[key]['x']=[]
            IS[key]['y']=[]

            for Rs in RIDS:
                IS[key]['x'].extend(px[np.where(CL_RID == Rs)].tolist())
                IS[key]['y'].extend(py[np.where(CL_RID == Rs)].tolist())
        #package in dataframe
        for key in IS:
            for p in range(len(IS[key]['x'])):
                entry = pd.DataFrame.from_dict({
                    "ID": [key],
                    "x":  [IS[key]['x'][p]],
                    "y":  [IS[key]['y'][p]]
                    })
                df = pd.concat([df, entry], ignore_index=True)

       #plot with folium
        mm = folium.Map(
            location=[df.y.mean(), df.x.mean()],
            tiles='Stamen Toner',
            zoom_start=5
            )
        colors = [ 'red', 'blue', 'gray', 'darkred', 'lightred', 'orange', 'beige', 'green', 'darkgreen',
        'lightgreen', 'darkblue', 'lightblue', 'purple', 'darkpurple', 'pink', 'cadetblue', 'lightgray', 'black' ]
        cc=0
        for pt in range(len(df)):
            #change color with group change
            if pt>0:
                if df.ID[pt] != df.ID[pt-1]:
                    cc=random.randrange(0,len(colors),1)

            folium.CircleMarker(([df["y"][pt],df["x"][pt]]), radius=3, weight=2, color=colors[cc], fill_color=colors[cc], fill_opacity=.5).add_to(mm)

        #save the map as an html    
        fname=self.params['algo'] + '.html'
        mm.save(fname)
        webbrowser.open(fname)

    def write_inversion_set_data(self,InversionSets,OutputDir):
        out_json = OutputDir / self.params['Filename']

        # these should be the same for each reach in the reaches file
        swordfile=self.reaches[0]['sword']        
        #swordfile=swordfile[0:2]+'_sword_v16_patch.nc'        
        sosfile=self.reaches[0]['sos']

        InversionSetsWrite=self.get_IS_list(InversionSets,swordfile,sosfile,'writing')        

        with open(out_json, 'w') as json_file:
            json.dump(InversionSetsWrite, json_file, indent=2)

    def get_IS_list(self,InversionSets,swordfile,sosfile,mode):
        #makes a list of inversion sets, where each list item is a another list of inversion set data
        #each inversion set list item is a dict of the data for each reach

        InversionSetsList=[]
        for IS in InversionSets:
            InversionSetWrite=[]
            for reach in InversionSets[IS]['ReachList']:
                reachdict={}
                reachdict['reach_id']=int(reach)
                reachdict['sword']=swordfile
                reachdict['swot']=str(reach) + '_SWOT.nc'
                reachdict['sos']=sosfile
                if mode == 'remove_dupes':
                    reachdict['origin']=InversionSets[IS]['OriginReach']['reach_id']
                InversionSetWrite.append(reachdict)
            InversionSetsList.append(InversionSetWrite)
        
        return InversionSetsList

    def print_stats(self,InversionSets):
        # output some stats
        numReaches=[]
        for IS in InversionSets:
            numReaches.append(InversionSets[IS]['numReaches'])
            
        # print('histogram of number of reaches in set')
        # plt.hist(numReaches)
        # plt.show()
            
        reaches_in_sets=[]
        for IS in InversionSets:
            for reachid in InversionSets[IS]['ReachList']:
                reaches_in_sets.append(reachid)        
                
        reaches_specified=[]
        for reach in self.reaches:
            reaches_specified.append(reach['reach_id'])                                                
        
        numOverlap=len( list( set(reaches_in_sets) & set(reaches_specified) ) )                            
        
        print('    total number of reaches:',len(self.reaches))
        print('    A total of', len(InversionSets.keys()),'sets were identified.')
        print('    Total reaches included in sets:',sum(numReaches))
        print('    Of the ',len(self.reaches),' reaches input to SetFinder, ',numOverlap,' are included in the sets')
        

    def getsets(self):
        # extract continent data into dict
        print('extracting data...')
        swordreachids,sword_data_continent=self.extract_data_sword_continent_file()                

        # get an inversion set for each reach
        print('getting inversion set for each reach...')
        # print(sword_data_continent,swordreachids)
        InversionSets=self.extract_inversion_sets_by_reach(sword_data_continent,swordreachids)
                       
        # remove sets with non-river reaches (this should now be superfluous)
        print('removing sets with non-river reaches...')
        InversionSets=self.remove_sets_with_non_river_reaches(InversionSets)
        
                
        # remove sets with too few reaches
        print('removing sets with too few reaches...')
        InversionSets=self.remove_small_sets(InversionSets)                    
        
        # remove duplicate or high-overlap sets
        print('removing overlapping or high overlap sets...')
        InversionSets=self.remove_duplicate_or_high_overlap_sets(InversionSets,swordreachids,sword_data_continent)
                
        # add single-reach sets to ensure all reaches are in a set (if specified in option)
        if self.params['MinimumReaches']==1:
            print('adding in sets with a single reach...')
            InversionSets=self.add_single_reach_sets(InversionSets,swordreachids,sword_data_continent)

        # stats
        print('print stats...')
        self.print_stats(InversionSets)

        # map
        #self.MKmap(InversionSets)
    
        return InversionSets


