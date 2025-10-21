# Created by: Ben Gannon and Matt Panunto
# Created on: 09/01/2025
# Last Updated: 10/05/2025

'''
Updates RAWS and PSA ERC and BI Percentiles and Trends tables using NFDRS observations and
forecasts from FEMS.
'''

# Import libraries and modules
import arcgis, os, sys, pandas, datetime, requests, json, statistics
from datetime import datetime, timedelta
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from time import sleep

# Working directory (currently only for log file)
wdir = 'XXXXXX'

# ArcGIS Online Portal URL
agol_portalurl = 'https://www.arcgis.com'

# AGOL service info
itemid = 'XXXXXX'
agol_username = 'XXXXXX'
agol_password = 'XXXXXX'

# Update date and time
# Or set manually set strings with formats 'YYYY-MM-DD' and 'HHMM'
udate = datetime.today().strftime('%Y-%m-%d')
utime = datetime.today().strftime('%H') + '00'
# udate = '2025-09-11'
# utime = '0200'

# Start log file and define function to print to both
lf = open(wdir + '/NFDRS_log_' + udate.replace('-','') + '.txt', 'w')
def print_both(ptext):
    print(ptext)
    lf.write(ptext)

#####################################################################################################
### Connect to AGOL service for required base data
#####################################################################################################
print_both('\r')
print_both('Connect to AGOL service for required base data\r')

# Establish connection to the ArcGIS Online Org
gis = GIS(agol_portalurl,agol_username,agol_password)
print_both('.Connected to AGOL\r')

# Connect to feature service
service = gis.content.get(itemid)
tables = service.tables
print_both('.Connected to feature service\r')

# Get Percentiles table
per_df = tables[0].query().df

# Get PSA_RAWS_Associations table
pra_df = tables[1].query().df

# Get RAWS_Percentiles_Trends table
whereClause = '"' + 'Station_ID' + '"' + ' IN ' + str(tuple(pra_df['Station_ID'].tolist()))
raws_df = tables[2].query(whereClause).df

# Get PSA_Percentiles_Trends table
whereClause = '"' + 'PSANationalCode' + '"' + ' IN ' + str(tuple(pra_df['PSA'].tolist()))
psa_df = tables[3].query(whereClause).df

#####################################################################################################
### Grab NFDRS observations and forecasts from FEMS
#####################################################################################################
print_both('\r')
print_both('Grab NFDRS observations and forecasts from FEMS\r')

# Define dates for observation and forecast periods
# Updates will be done in early AM Mountain Time, so observation will be yesterday and forecast will
# be today
o_sdate = (datetime.strptime(udate,'%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d') 
o_edate = (datetime.strptime(udate,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
f_sdate = udate
f_edate = (datetime.strptime(udate,'%Y-%m-%d') + timedelta(days=2)).strftime('%Y-%m-%d')

# API URL and query
FEMS_API = 'https://fems.fs2c.usda.gov/api/climatology/graphql'
qFDDE = 'query NfdrMinMax {\n\
  nfdrMinMax(\n' + \
'    startDate: ' + '\"{}\"'.format(o_sdate) + ',\n' + \
'    endDate: ' + '\"{}\"'.format(f_edate) + ',\n' + \
'	fuelModels: "Y"\n\
	stationIds: ""\n\
  ) {\n\
    _metadata {\n\
      page\n\
      per_page\n\
      total_count\n\
      page_count\n\
    }\n\
    data {\n\
      station_id\n\
      summary_date\n\
      nfdr_type\n\
      fuel_model\n\
      energy_release_component_max\n\
      burning_index_max\n\
    }\n\
  }\n\
}'

# Make data request up to 5 times, else quit program
fems_download = False
for i in range(0,5): # Try update up to 5 times
    try:
        response = requests.post(FEMS_API,json={'query': qFDDE})
        if(response.status_code == 200):
            data = json.loads(response.text)
            fd_df = pandas.json_normalize(data,record_path=['data','nfdrMinMax','data'])
            fd_df.rename(columns={'summary_date': 'date',
                                  'energy_release_component_max': 'ERC',
                                  'burning_index_max': 'BI'},
                         inplace=True)
            fems_download = True
    except:
        pass
    if fems_download == False:
        print_both('..Download failed, re-trying\r')
        sleep(30) # Wait 30 seconds before trying again
    else:
        break
if fems_download == False:
    print_both('..FEMS data failed to download after 5 attempts\r')
    print_both('Analysis and update aborted!\r')
    lf.close() # Close log file
    exit() # Terminate code

# Optional - save FEMS data export
# fd_df.to_csv(wdir + '/fems_data_' + udate.replace('-','') + '.csv')

#####################################################################################################
### RAWS NFDRS Percentiles and 3-DAY Trends
#####################################################################################################
print_both('\r')
print_both('RAWS NFDRS Percentiles and 3-DAY Trends\r')

# Create table to store RAWS data needed for PSA analysis 
raws2psa_df = pra_df[['Station_ID','Station_Name']].drop_duplicates()
raws2psa_df = raws2psa_df.reset_index(drop=True)
raws2psa_df['ERC_obs_per'] = pandas.NA
raws2psa_df['ERC_obs_start'] = pandas.NA
raws2psa_df['ERC_obs_end'] = pandas.NA
raws2psa_df['ERC_fcast_per'] = pandas.NA
raws2psa_df['ERC_fcast_start'] = pandas.NA
raws2psa_df['ERC_fcast_end'] = pandas.NA
raws2psa_df['BI_obs_per'] = pandas.NA
raws2psa_df['BI_obs_start'] = pandas.NA
raws2psa_df['BI_obs_end'] = pandas.NA
raws2psa_df['BI_fcast_per'] = pandas.NA
raws2psa_df['BI_fcast_start'] = pandas.NA
raws2psa_df['BI_fcast_end'] = pandas.NA

for i in range(0,raws_df.shape[0]):

    print_both('.Processing ' + str(raws_df['Station_ID'][i]) + ', ' + raws_df['Station_Name'][i] + '\r')

    cSID = raws_df['Station_ID'][i]

    # Grab the station's ERC and BI Percentile tables
    erc_per = per_df.loc[(per_df['Station_ID'] == cSID) & (per_df['Component'] == 'ERC')]
    bi_per = per_df.loc[(per_df['Station_ID'] == cSID) & (per_df['Component'] == 'BI')]

    #################################################################################################
    ### ERC
    #################################################################################################
    print_both('..Processing ERC\r')

    # Extract observation and forecast start and end values
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_sdate)].shape[0] > 0):
        o_s = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_sdate)]['ERC'].iloc[0]
    else:
        o_s = pandas.NA
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_edate)].shape[0] > 0):
        o_e = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_edate)]['ERC'].iloc[0]
    else:
        o_e = pandas.NA
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_sdate)].shape[0] > 0):
        f_s = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_sdate)]['ERC'].iloc[0]
    else:
        f_s = pandas.NA
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_edate)].shape[0] > 0):
        f_e = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_edate)]['ERC'].iloc[0]
    else:
        f_e = pandas.NA
        
    # Calculate observation percentile
    if(pandas.notnull(o_e)):
        if((o_e >= min(erc_per['GreaterThanEqualTo'])) & (o_e < max(erc_per['LessThan']))):
            o_p = erc_per.loc[(erc_per['GreaterThanEqualTo'] <= o_e) & (erc_per['LessThan'] > o_e)]['Percentile'].iloc[0]
        if(o_e < min(erc_per['GreaterThanEqualTo'])):
            o_p = 0.01
        if(o_e >= max(erc_per['LessThan'])):
            o_p = 100.00
        print_both('...ERC of ' + str(o_e) + ' is ' + str(o_p) + ' percentile\r')
    else:
        o_p = pandas.NA
        print_both('...Missing data to calculate observation percentile\r')

    # Classify observation trend
    if(pandas.notnull(o_s) & pandas.notnull(o_e)):
        es_diff = o_e - o_s
        if(es_diff >= 3):
            o_t = 'Increase'
        if(es_diff <= -3):
            o_t = 'Decrease'    
        if(abs(es_diff) < 3):
            o_t = 'No Change'
        print_both('...ERC from ' + str(o_s) + ' to ' + str(o_e) + ' has trend of ' + o_t + '\r')
    else:
        o_t = pandas.NA
        print_both('...Missing data to calculate observation trend\r')

    # Calculate forecast percentile
    if(pandas.notnull(f_s)):
        if((f_s >= min(erc_per['GreaterThanEqualTo'])) & (f_s < max(erc_per['LessThan']))):
            f_p = erc_per.loc[(erc_per['GreaterThanEqualTo'] <= f_s) & (erc_per['LessThan'] > f_s)]['Percentile'].iloc[0]
        if(f_s < min(erc_per['GreaterThanEqualTo'])):
            f_p = 0.01
        if(f_s >= max(erc_per['LessThan'])):
            f_p = 100.00
        print_both('...ERC forecast of ' + str(f_s) + ' is ' + str(f_p) + ' percentile\r')
    else:
        f_p = pandas.NA
        print_both('...Missing data to calculate forecast percentile\r')
        
    # Classify forecast trend
    if(pandas.notnull(f_s) & pandas.notnull(f_e)):
        es_diff = f_e - f_s
        if(es_diff >= 3):
            f_t = 'Increase'
        if(es_diff <= -3):
            f_t = 'Decrease'    
        if(abs(es_diff) < 3):
            f_t = 'No Change'
        print_both('...ERC forecast from ' + str(f_s) + ' to ' + str(f_e) + ' has trend of ' + f_t + '\r')
    else:
        f_t = pandas.NA
        print_both('...Missing data to calculate forecast trend\r')

    # Populate results data frame
    raws_df.loc[(raws_df['Station_ID'] == cSID),'erc'] = o_e
    raws_df.loc[(raws_df['Station_ID'] == cSID),'erc_percentile'] = o_p
    raws_df.loc[(raws_df['Station_ID'] == cSID),'erc_trend'] = o_t
    raws_df.loc[(raws_df['Station_ID'] == cSID),'erc_fcast'] = f_s
    raws_df.loc[(raws_df['Station_ID'] == cSID),'erc_fcast_percentile'] = f_p
    raws_df.loc[(raws_df['Station_ID'] == cSID),'erc_fcast_trend'] = f_t

    # Populate data needed for PSA analysis
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'ERC_obs_per'] = o_p
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'ERC_obs_start'] = o_s
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'ERC_obs_end'] = o_e
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'ERC_fcast_per'] = f_p
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'ERC_fcast_start'] = f_s
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'ERC_fcast_end'] = f_e
    
    #################################################################################################
    ### BI
    #################################################################################################
    print_both('..Processing BI\r')

    # Extract observation and forecast start and end values
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_sdate)].shape[0] > 0):
        o_s = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_sdate)]['BI'].iloc[0]
    else:
        o_s = pandas.NA
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_edate)].shape[0] > 0):
        o_e = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == o_edate)]['BI'].iloc[0]
    else:
        o_e = pandas.NA
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_sdate)].shape[0] > 0):
        f_s = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_sdate)]['BI'].iloc[0]
    else:
        f_s = pandas.NA
    if(fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_edate)].shape[0] > 0):
        f_e = fd_df.loc[(fd_df['station_id'] == cSID) & (fd_df['date'] == f_edate)]['BI'].iloc[0]
    else:
        f_e = pandas.NA
        
    # Calculate observation percentile
    if(pandas.notnull(o_e)):
        if((o_e >= min(bi_per['GreaterThanEqualTo'])) & (o_e < max(bi_per['LessThan']))):
            o_p = bi_per.loc[(bi_per['GreaterThanEqualTo'] <= o_e) & (bi_per['LessThan'] > o_e)]['Percentile'].iloc[0]
        if(o_e < min(bi_per['GreaterThanEqualTo'])):
            o_p = 0.01
        if(o_e >= max(bi_per['LessThan'])):
            o_p = 100.00
        print_both('...BI of ' + str(o_e) + ' is ' + str(o_p) + ' percentile\r')
    else:
        o_p = pandas.NA
        print_both('...Missing data to calculate observation percentile\r')

    # Classify observation trend
    if(pandas.notnull(o_s) & pandas.notnull(o_e)):
        es_diff = o_e - o_s
        if(es_diff >= 3):
            o_t = 'Increase'
        if(es_diff <= -3):
            o_t = 'Decrease'    
        if(abs(es_diff) < 3):
            o_t = 'No Change'
        print_both('...BI from ' + str(o_s) + ' to ' + str(o_e) + ' has trend of ' + o_t + '\r')
    else:
        o_t = pandas.NA
        print_both('...Missing data to calculate observation trend\r')

    # Calculate forecast percentile
    if(pandas.notnull(f_s)):
        if((f_s >= min(bi_per['GreaterThanEqualTo'])) & (f_s < max(bi_per['LessThan']))):
            f_p = bi_per.loc[(bi_per['GreaterThanEqualTo'] <= f_s) & (bi_per['LessThan'] > f_s)]['Percentile'].iloc[0]
        if(f_s < min(bi_per['GreaterThanEqualTo'])):
            f_p = 0.01
        if(f_s >= max(bi_per['LessThan'])):
            f_p = 100.00
        print_both('...BI forecast of ' + str(f_s) + ' is ' + str(f_p) + ' percentile\r')
    else:
        f_p = pandas.NA
        print_both('...Missing data to calculate forecast percentile\r')
        
    # Classify forecast trend
    if(pandas.notnull(f_s) & pandas.notnull(f_e)):
        es_diff = f_e - f_s
        if(es_diff >= 3):
            f_t = 'Increase'
        if(es_diff <= -3):
            f_t = 'Decrease'    
        if(abs(es_diff) < 3):
            f_t = 'No Change'
        print_both('...BI forecast from ' + str(f_s) + ' to ' + str(f_e) + ' has trend of ' + f_t + '\r')
    else:
        f_t = pandas.NA
        print_both('...Missing data to calculate forecast trend\r')

    # Populate results data frame
    raws_df.loc[(raws_df['Station_ID'] == cSID),'bi'] = o_e
    raws_df.loc[(raws_df['Station_ID'] == cSID),'bi_percentile'] = o_p
    raws_df.loc[(raws_df['Station_ID'] == cSID),'bi_trend'] = o_t
    raws_df.loc[(raws_df['Station_ID'] == cSID),'bi_fcast'] = f_s
    raws_df.loc[(raws_df['Station_ID'] == cSID),'bi_fcast_percentile'] = f_p
    raws_df.loc[(raws_df['Station_ID'] == cSID),'bi_fcast_trend'] = f_t

    # Fill in update date and time
    raws_df.loc[(raws_df['Station_ID'] == cSID),'update_date'] = udate
    raws_df.loc[(raws_df['Station_ID'] == cSID),'update_time'] = utime

    # Populate data needed for PSA analysis
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'BI_obs_per'] = o_p
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'BI_obs_start'] = o_s
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'BI_obs_end'] = o_e
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'BI_fcast_per'] = f_p
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'BI_fcast_start'] = f_s
    raws2psa_df.loc[(raws2psa_df['Station_ID'] == cSID),'BI_fcast_end'] = f_e

# Optional - save RAWS data
# raws_df.to_csv(wdir + '/raws_data_' + udate.replace('-','') + '.csv')
# raws2psa_df.to_csv(wdir + '/raws2psa_data_' + udate.replace('-','') + '.csv') # For testing


#####################################################################################################
### PSA NFDRS Percentiles and 3-DAY Trends
#####################################################################################################
print_both('\r')
print_both('PSA NFDRS Percentiles and 3-DAY Trends\r')

# Get list of PSAs to update
PSAs = sorted(list(set(pra_df['PSA'].tolist())))
PSAs = [PSA for PSA in PSAs if PSA != 'Non-PSA'] # Ignore non-PSA stations

for i in range(0,len(PSAs)):

    print_both('.Processing PSA ' + PSAs[i] + '\r')
    
    # Get list of RAWS in PSA
    stations = pra_df.loc[pra_df['PSA'] == PSAs[i],'Station_ID'].tolist()
        
    #################################################################################################
    ### ERC
    #################################################################################################
    print_both('..Processing ERC\r')
    
    # Extract observation and forecast start and end values
    o_s = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'ERC_obs_start'].dropna().tolist()
    o_e = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'ERC_obs_end'].dropna().tolist()
    o_p = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'ERC_obs_per'].dropna().tolist()
    f_s = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'ERC_fcast_start'].dropna().tolist()
    f_e = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'ERC_fcast_end'].dropna().tolist()
    f_p = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'ERC_fcast_per'].dropna().tolist()

    # Calculate observation mean
    if(len(o_e) > 0):
        o_e_mean = round(statistics.mean(o_e),2)
        print_both('...ERC mean: ' + str(o_e_mean) + '\r')
    else:
        o_e_mean = pandas.NA
        print_both('...ERC mean: no observations\r')
        
    # Calculate observation percentile mean
    if(len(o_p) > 0):
        o_p_mean = round(statistics.mean(o_p),2)
        print_both('...ERC percentile mean: ' + str(o_p_mean) + '\r')
    else:
        o_p_mean = pandas.NA
        print_both('...ERC percentile mean: no observations\r')
        
    # Classify observation trend
    if((len(o_s) > 0) & (len(o_e) > 0)):
        s_mean = round(statistics.mean(o_s),2)
        e_mean = round(statistics.mean(o_e),2)
        es_diff = e_mean - s_mean
        if(es_diff >= 3):
            o_t = 'Increase'
        if(es_diff <= -3):
            o_t = 'Decrease'    
        if(abs(es_diff) < 3):
            o_t = 'No Change'
        print_both('...ERC from ' + str(s_mean) + ' to ' + str(e_mean) + ' has trend of ' + o_t + '\r')
    else:
        o_t = pandas.NA
        print_both('...Missing data to calculate observation trend\r')

    # Calculate forecast mean
    if(len(f_s) > 0):
        f_s_mean = round(statistics.mean(f_s),2)
        print_both('...ERC forecast mean: ' + str(f_s_mean) + '\r')
    else:
        f_s_mean = pandas.NA
        print_both('...ERC forecast mean: no observations\r')

    # Calculate forecast percentile mean
    if(len(f_p) > 0):
        f_p_mean = round(statistics.mean(f_p),2)
        print_both('...ERC forecast percentile mean: ' + str(f_p_mean) + '\r')
    else:
        f_p_mean = pandas.NA
        print_both('...ERC forecast percentile mean: no observations\r')

    # Classify forecast trend
    if((len(f_s) > 0) & (len(f_e) > 0)):
        s_mean = round(statistics.mean(f_s),2)
        e_mean = round(statistics.mean(f_e),2)
        es_diff = e_mean - s_mean
        if(es_diff >= 3):
            f_t = 'Increase'
        if(es_diff <= -3):
            f_t = 'Decrease'    
        if(abs(es_diff) < 3):
            f_t = 'No Change'
        print_both('...ERC forecast from ' + str(s_mean) + ' to ' + str(e_mean) + ' has trend of ' + f_t + '\r')
    else:
        f_t = pandas.NA
        print_both('...Missing data to calculate forecast trend\r')
    
    # Populate results data frame
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_erc'] = o_e_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_erc_percentile'] = o_p_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_erc_trend'] = o_t
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_erc_fcast'] = f_s_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_erc_fcast_percentile'] = f_p_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_erc_fcast_trend'] = f_t

    #################################################################################################
    ### BI
    #################################################################################################
    print_both('..Processing BI\r')
    
    # Extract observation and forecast start and end values
    o_s = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'BI_obs_start'].dropna().tolist()
    o_e = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'BI_obs_end'].dropna().tolist()
    o_p = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'BI_obs_per'].dropna().tolist()
    f_s = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'BI_fcast_start'].dropna().tolist()
    f_e = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'BI_fcast_end'].dropna().tolist()
    f_p = raws2psa_df.loc[raws2psa_df['Station_ID'].isin(stations),'BI_fcast_per'].dropna().tolist()

    # Calculate observation mean
    if(len(o_e) > 0):
        o_e_mean = round(statistics.mean(o_e),2)
        print_both('...BI mean: ' + str(o_e_mean) + '\r')
    else:
        o_e_mean = pandas.NA
        print_both('...BI mean: no observations\r')
        
    # Calculate observation percentile mean
    if(len(o_p) > 0):
        o_p_mean = round(statistics.mean(o_p),2)
        print_both('...BI percentile mean: ' + str(o_p_mean) + '\r')
    else:
        o_p_mean = pandas.NA
        print_both('...BI percentile mean: no observations\r')
        
    # Classify observation trend
    if((len(o_s) > 0) & (len(o_e) > 0)):
        s_mean = round(statistics.mean(o_s),2)
        e_mean = round(statistics.mean(o_e),2)
        es_diff = e_mean - s_mean
        if(es_diff >= 3):
            o_t = 'Increase'
        if(es_diff <= -3):
            o_t = 'Decrease'    
        if(abs(es_diff) < 3):
            o_t = 'No Change'
        print_both('...BI from ' + str(s_mean) + ' to ' + str(e_mean) + ' has trend of ' + o_t + '\r')
    else:
        o_t = pandas.NA
        print_both('...Missing data to calculate observation trend\r')

    # Calculate forecast mean
    if(len(f_s) > 0):
        f_s_mean = round(statistics.mean(f_s),2)
        print_both('...BI forecast mean: ' + str(f_s_mean) + '\r')
    else:
        f_s_mean = pandas.NA
        print_both('...BI forecast mean: no observations\r')

    # Calculate forecast percentile mean
    if(len(f_p) > 0):
        f_p_mean = round(statistics.mean(f_p),2)
        print_both('...BI forecast percentile mean: ' + str(f_p_mean) + '\r')
    else:
        f_p_mean = pandas.NA
        print_both('...BI forecast percentile mean: no observations\r')

    # Classify forecast trend
    if((len(f_s) > 0) & (len(f_e) > 0)):
        s_mean = round(statistics.mean(f_s),2)
        e_mean = round(statistics.mean(f_e),2)
        es_diff = e_mean - s_mean
        if(es_diff >= 3):
            f_t = 'Increase'
        if(es_diff <= -3):
            f_t = 'Decrease'    
        if(abs(es_diff) < 3):
            f_t = 'No Change'
        print_both('...BI forecast from ' + str(s_mean) + ' to ' + str(e_mean) + ' has trend of ' + f_t + '\r')
    else:
        f_t = pandas.NA
        print_both('...Missing data to calculate forecast trend\r')

    # Populate results data frame
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_bi'] = o_e_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_bi_percentile'] = o_p_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_bi_trend'] = o_t
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_bi_fcast'] = f_s_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_bi_fcast_percentile'] = f_p_mean
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'avg_bi_fcast_trend'] = f_t

    # Fill in update date and time
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'update_date'] = udate
    psa_df.loc[(psa_df['PSANationalCode'] == PSAs[i]),'update_time'] = utime

# Optional - save PSA data
# psa_df.to_csv(wdir + '/psa_data_' + udate.replace('-','') + '.csv')


#####################################################################################################
### UPDATE SERVICE
#####################################################################################################
print_both('\r')
print_both('Updating service\r')

# Fill NAs in dataframes with values of None, else will throw an error when updating service
raws_df = raws_df.replace({pandas.NA: None})
psa_df = psa_df.replace({pandas.NA: None})

# Update RAWS table
print_both('.RAWS table\r')
raws_upload = False
for i in range(0,5): # Try update up to 5 times
    try:
        tbl = tables[2]
        # tbl.properties.capabilities # Make sure editing enabled
        fset = arcgis.features.FeatureSet.from_dataframe(raws_df)
        g = tbl.edit_features(updates=fset)
        raws_upload = True
    except:
        pass
    if raws_upload == False:
        print_both('..Upload failed, re-trying\r')
        sleep(30) # Wait 30 seconds before trying again
    else:
        break
if raws_upload == False:
    print_both('..RAWS table failed to update after 5 attempts\r')

# Update PSA table
print_both('.PSA table\r')
psa_upload = False
for i in range(0,5): # Try update up to 5 times
    try:
        tbl = tables[3]
        # tbl.properties.capabilities # Make sure editing enabled
        fset = arcgis.features.FeatureSet.from_dataframe(psa_df)
        g = tbl.edit_features(updates=fset)
        psa_upload = True
    except:
        pass
    if psa_upload == False:
        print_both('..Upload failed, re-trying\r')
        sleep(30) # Wait 30 seconds before trying again
    else:
        break
if psa_upload == False:
    print_both('..PSA table failed to update after 5 attempts\r')

print_both('\r')
print_both('Script complete!\r')

# Close log file
lf.close()
