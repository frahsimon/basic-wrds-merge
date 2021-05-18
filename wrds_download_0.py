# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 11:53:57 2021

@author: frede
"""

# BASIC MERGE SCRIPT
# NOTES: This script downloads and merges annual data in a very basic manner.
#        It annualizes crsp returns. It

###################################
### STEP 0: PACKAGES ##############
###################################

import wrds
import pandas as pd
import numpy as np


###################################
### STEP 1: DOWNLOAD DATA #########
###################################

db = wrds.Connection()

# compustat
comp_funda = db.raw_sql('''
                        SELECT gvkey, datadate, fyear, ib
                        FROM comp.funda
                        WHERE (indfmt='INDL')
                        AND (datafmt='STD')
                        AND (popsrc='D')
                        AND (consol='C')
                        AND (curcd = 'USD')
                        AND (fic = 'USA')
                        ''')

# crsp linktable; using ccm linktable (e.g. Zimmermann & Chen do so in their
# database)
crsp_linktable = db.raw_sql('''
                            SELECT lpermno AS permno, lpermco AS permco, gvkey, linkdt, linkenddt, linktype
                            FROM crsp.ccmxpf_linktable
                            ''')
        
# crsp data
crsp_msf = db.raw_sql('''
                      SELECT permno, permco, date, ret
                      FROM crsp.msf
                      WHERE date > '1988-01-01' 
                      ''')

# ibes linktable
ibes_linktable = db.raw_sql('''
                            SELECT *
                            FROM wrdsapps.ibcrsphist
                            ''')

# ibes
## NOTE: Using adjusted data for now. Probably have to chance to unadjusted data.
ibes_statsum = db.raw_sql('''
                          SELECT ticker, cusip, cname, fiscalp, statpers, fpedats, actual, meanest, medest, stdev
                          FROM ibes.statsum_epsus
                          
                          WHERE EXTRACT(MONTH FROM fpedats) - EXTRACT(MONTH FROM statpers) = 
                          
                                  CASE
                                  WHEN EXTRACT(MONTH FROM fpedats) >= 9
                                      THEN 9
                                  ELSE 3
                                  END
                                  
                          AND EXTRACT(YEAR FROM fpedats) - EXTRACT(YEAR FROM statpers) = 
                                  
                                  CASE 
                                  WHEN EXTRACT(MONTH FROM fpedats) >= 9
                                      THEN 0
                                  ELSE 1
                                  END
                          
                          AND fiscalp = 'ANN';
                          ''')


####################################
### STEP 2: MERGE GVKEY TO CRSP ####
####################################

# merge crsp link via pandas
temp_df = crsp_msf.merge(crsp_linktable, on=['permno', 'permco'], how='left') # merge on permno
temp_df = temp_df[(temp_df.linktype == 'LU') | (temp_df.linktype == 'LC')] # only keep linktype LU or LC
temp_df = temp_df[(temp_df.date >= temp_df.linkdt) & (temp_df.date <= temp_df.linkenddt)] # only keep links that lie in range


####################################
### STEP 3: MERGE COMPUSTAT TO CRSP 
####################################

# We need to do this now, since we need the compustat fiscal year information in order to properly annualize the crsp returns.

# merge on year + month 
# first extract year + month from crsp date
temp_df['year'] = pd.DatetimeIndex(temp_df['date']).year
temp_df['month'] = pd.DatetimeIndex(temp_df['date']).month

# then extract year + month from compustat datadate
comp_funda['year'] = pd.DatetimeIndex(comp_funda['datadate']).year 
comp_funda['month'] = pd.DatetimeIndex(comp_funda['datadate']).month

# merge
temp_df = temp_df.merge(comp_funda, on=['gvkey', 'year', 'month'], how='left')

temp_df['fyear_fill'] = temp_df['fyear']

def fill(x):
    x = x.bfill()
    return x
    
temp_df.fyear_fill = temp_df.groupby('gvkey')['fyear_fill'].transform(fill)

temp_df['ret_plus1'] = temp_df['ret'] + 1 # ret + 1

# count variable indicating months per fiscal year/calendar year available
temp_1 = temp_df.groupby(['gvkey', 'permno', 'permco', 'fyear_fill']).month.count()

temp_1 = temp_1.reset_index()

temp_1 = temp_1.rename({'month': 'count_mpfyear'}, axis=1)

temp_2 = temp_df.groupby(['gvkey', 'permno', 'permco', 'year']).month.count()

temp_2 = temp_2.reset_index()

temp_2 = temp_2.rename({'month': 'count_mpcyear'}, axis=1)

temp_df = temp_df.merge(temp_1, on=['gvkey', 'permno', 'permco', 'fyear_fill'], how='left')

temp_df = temp_df.merge(temp_2, on=['gvkey', 'permno', 'permco', 'year'], how='left')


####################################
### STEP 4: ANNUALIZE CRSP #########
####################################

# general issue: fiscal year =/= calendar year in many cases

# a: annualize per fiscal year
temp_df_a = temp_df.copy() # create (shallow, i.e. no need for deep since not a compound object) copy

temp_df_a['cum_return_plus1_fyear'] = temp_df_a.groupby(['permno','fyear_fill']).ret_plus1.cumprod()
temp_df_a = temp_df_a.groupby(['permno','fyear_fill']).tail(1)
temp_df_a['cum_return_fyear'] = temp_df_a['cum_return_plus1_fyear'] -1 

# b: annualize per calendar year (the calendar year which is equal to the respective fiscal year row)
temp_df_b = temp_df.copy() # create (shallow, i.e. no need for deep since not a compound object) copy

temp_df_b['cum_return_plus1_cyear'] = temp_df_b.groupby(['permno','year']).ret_plus1.cumprod()
temp_df_b = temp_df_b.groupby(['permno','year']).tail(1)
temp_df_b['cum_return_cyear'] = temp_df_b['cum_return_plus1_cyear'] -1 

temp_df_b = temp_df_b[['permno', 'permco', 'gvkey', 'year', 'cum_return_cyear']]

# merge returns per calendar year into a; 
temp_df = temp_df_a.merge(temp_df_b, left_on=['permno', 'permco', 'gvkey', 'year'], right_on=['permno', 'permco', 'gvkey', 'year'],
                          how='left') # here we need to merge on year = year!

temp_df = temp_df.drop(['cum_return_plus1_fyear', 'ret_plus1'], 1) # delete unnecessary columns

temp_df = temp_df.rename({'year': 'cyear'}, axis=1) 


#####################################
### STEP 5: MERGE TICKER TO DF ######
#####################################

# merge ibes link via pandas
temp_df = temp_df.merge(ibes_linktable, on='permno', how='left') # merge on permno
temp_df = temp_df[(temp_df.score < 5)] # only keep score < 5
temp_df = temp_df[(temp_df.datadate >= temp_df.sdate) & (temp_df.datadate <= temp_df.edate)] # only keep links that lie in range

backup = temp_df.copy() # create (shallow, i.e. no need for deep since not a compound object) copy

#####################################
### STEP 6: MERGE IBES TO DF ########
#####################################

ibes_statsum['mergeyear'] = pd.DatetimeIndex(ibes_statsum['fpedats']).year
ibes_statsum['mergemonth'] = pd.DatetimeIndex(ibes_statsum['fpedats']).month

temp_df['mergeyear'] = pd.DatetimeIndex(temp_df['datadate']).year
temp_df['mergemonth'] = pd.DatetimeIndex(temp_df['datadate']).month

temp_df = temp_df.merge(ibes_statsum, on=['mergeyear', 'mergemonth', 'ticker'], how='left')

test = temp_df[['gvkey', 'datadate']]

check = test[test.duplicated(keep=False)]


check = temp_df[temp_df.duplicated(keep=False)]

