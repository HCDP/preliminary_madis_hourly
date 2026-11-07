"""
Version 0.2: madis_hourly_aqs test deployment
Update notes: 09.2026
-- Added skn merge onto the table, null where no match exists
-- Deprecated log-file tracking in favor of requests
Update notes 08.2026
-- Update madis acquisition to use https via requests package
-- Add new lon/lat metadata fields
Objective:
-Run data acquisition from last acquisition through latest complete hour.
Workflow steps:
-Check master log file for last time

*Note: code will acquire at least one hour of data for each time it is launched.
    --At minimum acquires the previous complete hour of data
    --This code will never acquire data for the current incomplete hour
"""
import os
import pytz
import warnings
import traceback
import requests
import gzip
import io
import json
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from os.path import exists
from xarray import SerializationWarning
from util import handle_retry

warnings.filterwarnings('ignore',category=SerializationWarning)

#----ENVIRONMENT VARIABLES----#
ROOT_DIR = os.environ.get("PROJECT_ROOT")
API_TOKEN = os.environ.get("HCDP_API_TOKEN")
ST_TIME = os.environ.get("ACQ_START_TIME") #can be None object
EN_TIME = os.environ.get("ACQ_END_TIME") #can be None object
POST_CAP  = os.environ.get("POST_ROW_CAP") #can be None object, default 500
if POST_CAP is None:
    POST_CAP = 500
else:
    POST_CAP = int(POST_CAP)

#----INTERNAL STATICS----#
MASTER_LOG = os.path.join(ROOT_DIR,'dataCatalog','master_log_test.csv')
PARSED_DIR = os.path.join(ROOT_DIR,'data_out')
DATA_STREAM = 'MADIS'
DATA_INDEX = 'NWS.id'
SOURCE_INDEX = 'stationId'
DF_COLS = ['stationId','stationName','dataProvider','lon','lat','time','varname','value','units','source']
COORDINATE_KEYS = ['longitude','latitude']
META_VAR_KEYS = ['stationId','stationName','dataProvider','longitude','latitude','observationTime']
MESONET_DATA_VARS = ['temperature','dewpoint','relHumidity','stationPressure','seaLevelPressure','windDir','windSpeed','windGust','windDirMax','rawPrecip','precipAccum','solarRadiation','fuelTemperature','fuelMoisture','soilTemperature','soilMoisture','soilMoisturePercent','minTemp24Hour','maxTemp24Hour','windDir10','windSpeed10','windGust10','windDirMax10']
HFMETAR_DATA_VARS = ['temperature','dewpoint','windDir','windSpeed','windGust','rawPrecip','precipAccum']
K_CONVERSION_KEYS = ['temperature','dewpoint','fuelTemperature','soilTemperature','minTemp24Hour','maxTemp24Hour']
STR_CONVERSION_KEYS = ['stationId','stationName','dataProvider']
SRC_VAR_HASH = {'mesonet':MESONET_DATA_VARS,'hfmetar':HFMETAR_DATA_VARS}
MASTER_LINK = 'https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/Hawaii_Master_Station_Meta.csv'
LON_KEY = 'longitude'
LAT_KEY = 'latitude'
TIME_KEY = 'observationTime'
STR_FMT = "UTF-8"
MIN_LON = -160 #HI: -160 GU: 144.5
MAX_LON = -154 #HI:-154 GU: 145.117
MIN_LAT = 18 #HI: 18 GU: 13.167
MAX_LAT = 22.5 #HI: 22.5 GU: 13.75
K_CONST = 273.15
UTC_OFFSET = -10
LOOKBACK_DAYS = 5
ARCHIVE_ONLY_THRESHOLD_DAYS = 5
_MASTER_META_CACHE = None
BASE_API_URL = f"https://api.hcdp.ikewai.org/acquisition/madis/" #Fixed on madis

#----HELPERS----#
def make_get_request(url,params):
    """
    Wraps API get request for handle_retry. 
    """
    return requests.get(url,params,headers = {'Authorization': f'Bearer {API_TOKEN}'}, timeout = 10)

def make_post_request(url,data_packet):
    json_packet = {"data":json.loads(data_packet)}
    return requests.post(url,json=json_packet,headers = {'Authorization': f'Bearer {API_TOKEN}'},timeout=60)
    
def add_skn(src_id,conversion_key):
    if src_id in list(conversion_key.keys()):
        return conversion_key[src_id]
    else:
        return np.nan

def get_master_meta():
    """Loads MASTER_LINK once per script invocation and reuses it for every
    process_nc_data call in that run, instead of re-downloading it per
    source per hour."""
    global _MASTER_META_CACHE
    if _MASTER_META_CACHE is None:
        _MASTER_META_CACHE = pd.read_csv(MASTER_LINK,index_col=DATA_INDEX)
    return _MASTER_META_CACHE

#----API CALL FOR STATUS----#
def get_latest_hour(now):
    """
    Calls most recent data line in database. [Have to assume the data are sorted by time so that station differences will not cause issues].
    Input:
        - now: pandas datetime object, HST (tz naive)
    Output:
        - pandas datetime object, HST (tz naive)
    """
    # Fetch latest data point
    endpoint_url = f"{BASE_API_URL}data"
    params = {
        'limit':1
    }
    req = handle_retry(make_get_request,[endpoint_url,params])
    if req.status_code != 200:
        print("API request failed")
        return
    # Process to dataframe
    decoded = req.content.decode('utf-8')
    req_data = pd.read_json(io.StringIO(decoded),convert_dates=True)
    if req_data.empty:
        # If return code 200 but empty, assume fresh database, fetch most recent completed hour IN HST
        return pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)
    else:
        # limit:1 enforces single row if not empty
        timestamp = req_data['timestamp'].values[0]
        return pd.Timestamp(timestamp).floor('h')
#----API POST FORMATTER----#
def build_post_request(df):
    """
    Takes full fetched table and packages into JSON objects. Submits POST to three tables in batches of 5000 (or POST_CAP) rows.
    """
    data_url = f"{BASE_API_URL}data"
    var_meta_url = f"{BASE_API_URL}variableMetadata"
    sta_meta_url = f"{BASE_API_URL}stationMetadata"
    # Separate df into the three table setups
    sta_meta_df = df[["stationId","stationName","dataProvider","SKN","lat","lon","source"]]
    var_meta_df = df[["varname","units"]]
    data_df = df[["stationId","time","varname","value"]]

    # Convert column names according to unified db standards
    sta_meta_df = sta_meta_df.rename(columns={"stationId":"station_id","stationName":"station_name","dataProvider":"data_provider","SKN":"skn","lon":"lng"})
    var_meta_df = var_meta_df.rename(columns={"varname":"variable","units":"unit"})
    data_df = data_df.rename(columns={"stationId":"station_id","time":"timestamp","varname":"variable"})

    # De-dupe station-times
    sta_meta_df = sta_meta_df.drop_duplicates(subset=["station_id"])
    var_meta_df = var_meta_df.drop_duplicates()
    data_df = data_df.drop_duplicates(subset=["station_id","timestamp","variable"])

    # Convert timestamps to ISO8601
    data_df["timestamp"] = pd.to_datetime(data_df["timestamp"]).dt.tz_localize('UTC').apply(lambda t: t.isoformat(timespec="minutes"))

    # Now batch into rows of 5000
    # POST station_metadata
    print("posting station metadata")
    sta_meta_json = sta_meta_df.to_json(orient="records")
    req = handle_retry(make_post_request,[sta_meta_url,sta_meta_json])
    print(req.content)

    # POST variable_metadata
    print("posting variable metadata")
    var_meta_json = var_meta_df.to_json(orient="records")
    req = handle_retry(make_post_request,[var_meta_url,var_meta_json])
    print(req.content)

    # POST data
    data_batches = [
        data_df.iloc[start:start + POST_CAP].to_json(orient="records")
        for start in range(0, len(df), POST_CAP)
    ]
    for batch in data_batches:
        req = handle_retry(make_post_request,[data_url,batch])
        print(req.content)

    return
#----Madis raw netcdf processor functions----
#----------(NECESSARY TO READ FILE)----------
def build_ldad_url(src_prefix, utc_filename):
    return os.path.join(f"https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/{src_prefix}/netCDF/", utc_filename)

def build_archive_url(src_prefix, utc_hour, utc_filename):
    year, mon, day = utc_hour.strftime('%Y'), utc_hour.strftime('%m'), utc_hour.strftime('%d')
    return os.path.join(f"https://madis-data.ncep.noaa.gov/madisPublic1/data/archive/{year}/{mon}/{day}/LDAD/{src_prefix}/netCDF/", utc_filename)

def get_units(unit_dict,ds,source):
    #Internal function, defines units for preset variables
    var_keys = SRC_VAR_HASH[source]
    avail_vars = [vk for vk in var_keys if vk in list(ds.keys())]
    for vk in avail_vars:
        unit_dict[vk] = ds[vk].units
    
    for temp_key in K_CONVERSION_KEYS:
        if temp_key in avail_vars:
            unit_dict[temp_key] = 'celsius'

def extract_values(var_stack,ds,extract_vars,subset_index=None):
    #Internal function: Extracts all variables to var_stack that don't require conversion
    avail_vars = list(ds.keys())
    for vname in extract_vars:
        if vname in avail_vars:
            if subset_index is None:
                converted_array = ds[vname].values
            else:
                converted_array = ds[vname].values[subset_index]
            var_stack[vname] = converted_array
        
def convert_K2C(var_stack,ds,kelvin_vars,source,subset_index=None):
    #Input note: var_stack should pass by reference, assuming it works correctly
    #Internal function: Converts Kelvin-based variables to celsius and adds to var_stack
    avail_vars = list(ds.keys())
    for kv in kelvin_vars:
        if kv in avail_vars:
            if kv in SRC_VAR_HASH[source]:
                if subset_index is None:
                    converted_array = ds[kv].values - K_CONST
                else:
                    converted_array = ds[kv].values[subset_index] - K_CONST
                var_stack[kv] = converted_array
        
def convert_str(var_stack,ds,str_vars,subset_index=None):
    #Internal function: Converts string binaries to standard string data type
    avail_vars = list(ds.keys())
    for sv in str_vars:
        if sv in avail_vars:
            if subset_index is None:
                converted_array = ds[sv].str.decode(STR_FMT,errors='ignore').values
            else:
                converted_array = ds[sv].str.decode(STR_FMT,errors='ignore').values[subset_index]
            var_stack[sv] = converted_array

def convert_time(var_stack,ds,subset_index=None):
    #Internal function: Converts times to datetime type
    time = pd.to_datetime(ds[TIME_KEY].values).strftime('%Y-%m-%d %H:%M:%S')
    if subset_index is not None:
        time = time[subset_index]
    var_stack[TIME_KEY] = time

#----(END MADIS INGESTION HELPER GROUP)----

#----(ACTUAL PROCESSING FUNCTION)
def process_nc_data(ds,src):
    lon = ds['longitude']
    lat = ds['latitude']

    loni = np.where((lon>=MIN_LON) & (lon<=MAX_LON))
    lati = np.where((lat>=MIN_LAT) & (lat<=MAX_LAT))
    hii = np.intersect1d(loni,lati)

    data_var_keys = SRC_VAR_HASH[src]
    unit_dict = {}
    converted_dict = {}
    get_units(unit_dict,ds,src)
    extract_values(converted_dict,ds,data_var_keys,hii)
    extract_values(converted_dict,ds,COORDINATE_KEYS,hii)
    convert_K2C(converted_dict,ds,K_CONVERSION_KEYS,src,hii)
    convert_str(converted_dict,ds,STR_CONVERSION_KEYS,hii)
    convert_time(converted_dict,ds,hii)
    df = pd.DataFrame(columns=DF_COLS)
    avail_var_keys = [vk for vk in data_var_keys if vk in list(ds.keys())]
    for vk in avail_var_keys:
        meta_group = [[converted_dict[key][index] for key in META_VAR_KEYS] for index in range(len(hii))]
        varname = ((vk+' ')*len(hii)).split()
        units = ((unit_dict[vk]+' ')*len(hii)).split()
        src_col = [src for i in range(len(hii))]
        var_group = [[varname[index],converted_dict[vk][index],units[index],src_col[index]] for index in range(len(hii))]
        full_group = [meta_group[index]+var_group[index] for index in range(len(hii))]
        var_df = pd.DataFrame(full_group,columns=DF_COLS)
        df = pd.concat([df,var_df])
    
    #Clear all no-data values
    df = df[~df['value'].isna()]
    # Append SKNs
    ##  Load master
    master_df = get_master_meta()
    all_src_ids = df[SOURCE_INDEX].dropna().unique()
    matched_ids = np.intersect1d(master_df.index.dropna().values,all_src_ids)
    matched_skns = master_df.loc[matched_ids,"SKN"]

    dupe_mask = matched_skns.index.duplicated(keep=False)
    if dupe_mask.any():
        ambiguous_ids = sorted(set(matched_skns.index[dupe_mask]))
        print(f"Ambiguous NWS.id entries in master table this run (treated as unmatched): {ambiguous_ids}")
        matched_skns = matched_skns[~dupe_mask]
    conversion_dict = matched_skns.to_dict()
    df['SKN'] = df[SOURCE_INDEX].apply(add_skn,args=(conversion_dict,))
    return df

def update_csv(csvname,new_df):
    #Appends processed data to preexisting file if exists, creates new otherwise
    #concat operation so doesn't need to worry about reading an empty file
    if exists(csvname):
        prev_df = pd.read_csv(csvname)
        upd_df = pd.concat([new_df,prev_df],axis=0,ignore_index=True)
        upd_df = upd_df.drop_duplicates()
        upd_df = upd_df.fillna('NA')
        upd_df.to_csv(csvname,index=False)
    else:
        new_df = new_df.drop_duplicates()
        new_df = new_df.fillna('NA')
        new_df.to_csv(csvname,index=False)

def fetch_url(url):
    response = requests.get(url, stream=True)
    if response.status_code == 404:
        return None          # file absent — caller skips, no retry
    response.raise_for_status()  # raises on 5xx, triggering handle_retry
    return response

def resolve_madis_response(src_prefix, utc_hour, utc_filename, now):
    now_utc = now + pd.Timedelta(hours=10)
    age_days = (now_utc - utc_hour).total_seconds() / 86400
    ldad_url = build_ldad_url(src_prefix, utc_filename)
    archive_url = build_archive_url(src_prefix, utc_hour, utc_filename)
    primary, fallback = (ldad_url, archive_url) if age_days < ARCHIVE_ONLY_THRESHOLD_DAYS else (archive_url, ldad_url)

    response = handle_retry(fetch_url,(primary,))
    if response is not None:
        return response
    print(f"Not found at {primary}, trying {fallback}.")
    return handle_retry(fetch_url,(fallback,))
    
def madis_fetch_hour(hour,now):
    """
    Functionality: Pulls 1 hour of madis data, extracts Hawaii stations from
    netcdf and converts to long format pandas dataframe
    Input:
    --hour: Pandas datetime variable for target hour to fetch madis file
    Output:
    --Writes/appends to YYYYMMDD_madis_parsed.csv
    """

    #Get filenames by converting hour to utc
    targ_csv = os.path.join(PARSED_DIR,'_'.join((hour.strftime('%Y%m%d'),DATA_STREAM.lower(),'parsed'))+'.csv')
    utc_hour = hour - pd.Timedelta(hours=UTC_OFFSET)
    utc_filename = utc_hour.strftime('%Y%m%d_%H%M')+'.gz'

    src_flags = {}
    for src_prefix in list(SRC_VAR_HASH.keys()):
        http_root = f"https://madis-data.ncep.noaa.gov/madisPublic1/data/LDAD/{src_prefix}/netCDF/"
        url = os.path.join(http_root,utc_filename)
        print(f"Trying {src_prefix} {utc_filename}.")
        try:
            response = resolve_madis_response(src_prefix, utc_hour, utc_filename, now) #Keep retry count at default 10
        except Exception as e:
            print(f"Failed to fetch {utc_filename} from {src_prefix} after retries: {e}")
            print(traceback.format_exc())
            src_flags[src_prefix] = False
            continue
        if response is None:
            #404 response returned
            print("Response:",response)
            print(f"404, no file exists for hour {utc_hour.strftime('%Y-%m-%d %H%M')}")
            src_flags[src_prefix] = False
            continue
        try:
            with gzip.open(io.BytesIO(response.content)) as gz:
                ds = xr.open_dataset(gz)
                df = process_nc_data(ds,src_prefix)
                #update_csv(targ_csv,df)
        except Exception as e:
            print(f"Failed to fetch {utc_filename} from {src_prefix}: {e}")
            print(traceback.format_exc())
            src_flags[src_prefix] = False
            continue
        src_flags[src_prefix] = True
    
    #Return True if both succeed
    return all(src_flags.values()), df
    

if __name__=="__main__":
    # Check if environment variables have values
    hst = pytz.timezone('HST')
    now = pd.to_datetime(datetime.now(hst)).tz_localize(None)
    recent_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)
    batch_mode = False
    if (ST_TIME is None) & (EN_TIME is None):
        # If no specified input, check database for most recent data and backfill up until last completed hour (up to lookback_limit)
        latest_hour = get_latest_hour(now) + pd.Timedelta(hours=UTC_OFFSET)
        target_hour = latest_hour + pd.Timedelta(hours=1) 
        lookback_limit = now - pd.Timedelta(days=LOOKBACK_DAYS) + pd.Timedelta(hours=UTC_OFFSET)
        fetch_time = max(target_hour,lookback_limit)
        print("No specified input timestamps. Processing recent data from :",fetch_time)
        if fetch_time <= recent_hour:
            # Check if fetch_time from database query is farther back than the last completed hour.
            # Set batch specs to run fetch for each hour from fetch_time through recent_hour
            batch_st = fetch_time
            batch_en = recent_hour
            batch_mode = True
        else:
            # fetch_time == recent_hour
            print("Database is up to date. Specify hour input manually to force re-fetch.")
            quit()        
    elif any(((ST_TIME is None),(EN_TIME is None))):
        # If one of them is None but not the other (ie. one date supplied)
        input_time = ST_TIME if ST_TIME is not None else EN_TIME
        fetch_time = pd.to_datetime(input_time) # convert env string to datetime
        if fetch_time <= recent_hour:
            print("Received timestamp input. Processing for specified hour: ", fetch_time)
        else:
            print("Invalid timestamp input. Exiting.")
            quit()
    else:
        batch_st = pd.to_datetime(ST_TIME)
        batch_en = pd.to_datetime(EN_TIME)
        if (batch_st <= batch_en)&(batch_en <= recent_hour):
            batch_mode = True
            print(f"Received timestamp input range. Processing data from {batch_st} to {batch_en}.")
        else:
            print(f"Error in start and end times. Start time is greater than end time. Exiting.")
            quit()
    
    # If batch_mode, run from batch_st through batch_en
    if batch_mode:
        date_range = pd.date_range(batch_st,batch_en,freq="h")
        successes = []
        hour_data = []
        for dt in date_range:
            success, hour_df = madis_fetch_hour(dt,now)
            successes.append(success)
            hour_data.append(hour_df)
        print(f"{np.sum([int(s) for s in successes])}/{len(list(date_range))} successful fetches.")
        hour_data = pd.concat(hour_data,axis=0)
    else:
        success, hour_data = madis_fetch_hour(fetch_time,now)
        
    # Process hour_data into a POST request
    print("Submitting POST request")
    build_post_request(hour_data)