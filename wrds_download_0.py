# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 11:53:57 2021

@author: frede
"""

# BASIC MERGE SCRIPT
# NOTES: This script downloads and merges annual data in a very basic manner.
#        It annualizes crsp returns. It

###################################
### STEP 0: PACKAGES // PRE #######
###################################

import wrds
import pandas as pd
import sqlite3

db = wrds.Connection()

sql = sqlite3.connect(':memory:')
c = sql.cursor()


###################################
### STEP 1: DOWNLOAD DATA #########
###################################

# compustat
comp_funda = db.raw_sql('''
                        
                        SELECT gvkey, datadate, fyear, ib
                        FROM comp.funda
                        WHERE (indfmt='INDL')
                        AND (datafmt='STD')
                        AND (popsrc='D')
                        AND (consol='C')
                        AND (curcd = 'USD')
                        AND (fic = 'USA');
                        
                        ''')

# crsp linktable; using ccm linktable (e.g. Zimmermann & Chen do so in their
# database)
crsp_linktable = db.raw_sql('''
                            
                            SELECT lpermno AS permno, lpermco AS permco, gvkey, linkdt, linkenddt, linktype
                            FROM crsp.ccmxpf_linktable;
                            
                            ''')
        
# crsp data
crsp_msf = db.raw_sql('''
                      
                      SELECT permno, permco, date, ret
                      FROM crsp.msf; 
                      
                      ''')

# ibes linktable
ibes_linktable = db.raw_sql('''
                            
                            SELECT *
                            FROM wrdsapps.ibcrsphist;
                            
                            ''')

# ibes
## NOTE: Using adjusted data.
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


#################################################
### STEP 2: MERGE PERMNO/PERMCO TO DF (COMPUSTAT)
#################################################

# merge via sql
# linktype LU or LC
# only keep links that lie in the linkrange
comp_funda.to_sql('comp_funda', sql, if_exists='replace', index=False)
crsp_linktable.to_sql('crsp_linktable', sql, if_exists='replace', index=False)

temp_df_sql = c.execute('''
                        
                        SELECT a.*, b.permno, b.permco
                        FROM comp_funda AS a
                        LEFT JOIN crsp_linktable AS b
                        ON a.gvkey = b.gvkey
                        WHERE b.linktype = 'LU' OR 'LC'
                        AND a.datadate BETWEEN b.linkdt AND b.linkenddt
                        
                        ''')

temp_df = pd.DataFrame(temp_df_sql.fetchall())
temp_df.columns = list(map(lambda x: x[0], c.description)) # get column names from sql cursor


#################################################
### STEP 3: MERGE TICKER TO DF ##################
#################################################

ibes_linktable.to_sql('ibes_linktable_sql', sql, if_exists='replace', index=False)
temp_df.to_sql('temp_df_sql', sql, if_exists='replace', index=False)

temp_df_sql = c.execute('''
                             
                        SELECT a.*, b.ticker, b.score, b.sdate, b.edate
                        FROM temp_df_sql AS a
                        LEFT JOIN ibes_linktable_sql AS b
                        ON a.permno = b.permno
                        AND a.datadate BETWEEN b.sdate and b.edate
                        AND b.score < 5
                        
                        ''') 

temp_df = pd.DataFrame(temp_df_sql.fetchall())
temp_df.columns = list(map(lambda x: x[0], c.description))


#################################################
### STEP 4: MERGE CRSP TO DF ####################
#################################################

temp_df.to_sql('temp_df_sql', sql, if_exists='replace', index=False)
crsp_msf.to_sql('crsp_msf_sql', sql, if_exists='replace', index=False)

temp_df_sql = c.execute('''
                        
                          SELECT *, DATE(datadate, 'start of month', '-11 months') as datadate_start
                          FROM temp_df_sql
                          
                          ''')

# tbd this can be done more efficiently probably
temp_df_sql = pd.DataFrame(temp_df_sql.fetchall())
temp_df_sql.columns = list(map(lambda x: x[0], c.description))
temp_df_sql.to_sql('temp_df_sql', sql, if_exists='replace', index=False)

temp_df_sql = c.execute('''
                        
                        SELECT a.*, b.ret
                        FROM temp_df_sql AS a
                        LEFT JOIN crsp_msf_sql AS b
                        ON a.permno = b.permno
                        AND a.permno = b.permno
                        WHERE b.date BETWEEN a.datadate_start AND a.datadate
                        
                        ''')

temp_df = pd.DataFrame(temp_df_sql.fetchall())
temp_df.columns = list(map(lambda x: x[0], c.description))


#################################################
### STEP 5: MERGE IBES TO DF ####################
#################################################

temp_df.to_sql('temp_df_sql', sql, if_exists='replace', index=False)
ibes_statsum.to_sql('ibes_statsum_sql', sql, if_exists='replace', index=False)

temp_df_sql = c.execute('''
                        
                        SELECT a.*, b.fpedats, b.statpers, b.medest, b.meanest, b.stdev
                        FROM temp_df_sql AS a
                        LEFT JOIN ibes_statsum_sql AS b
                        ON a.ticker = b.ticker
                        AND a.datadate = b.fpedats
                                                
                        ''')

temp_df = pd.DataFrame(temp_df_sql.fetchall())
temp_df.columns = list(map(lambda x: x[0], c.description))


#################################################
### STEP 6: ANNUALIZE CRSP ######################
#################################################

# add indicator to show how many months of data per fiscal year
# tbd

# annualizing per fiscal year
temp_df['ret_plus1'] = temp_df['ret'] + 1

temp_df['cum_return_plus1'] = temp_df.groupby(['gvkey','permno','fyear']).ret_plus1.cumprod()
temp_df = temp_df.groupby(['permno','fyear']).tail(1)

temp_df['cum_return_fyear'] = temp_df['cum_return_plus1_fyear'] -1 

df = temp_df

