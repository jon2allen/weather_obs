import pandas as pd
import requests
from bs4 import BeautifulSoup
from weather_obs import * 

def parse_noaa_state_page(url, state):
    df = pd.DataFrame()
    try:
       response = requests.get(url)
    except:
       return df
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.findAll('table')

    records = []
    columns = []

    table = tables[6]
    # print(table)
    for tr in table.findAll("tr"):
        ths = tr.findAll("th")
        if ths != []:
            for each in ths:
                print("len_each", str(len(each.text)))
                if ( len(each.text) > 20 ):
                    columns.append(str(each.text[:20]))
                else:
                    columns.append(each.text)
                # print("col: ", str(each.text) )
            columns.append("State")
            columns.append("latitude")
            columns.append("longitude")
        else:
            trs = tr.findAll("td")
            # print("trs")
            # print(trs)
            # print("len: ", len(trs))
            record = []
            first_pass = 0
            trs_len = len(trs)
            for each in trs:
                if (trs_len < 3 ):
                    pass
                try:
                    link = each.find('a')['href']
                    text = each.text
                #record.append(link)
                    if (first_pass == 0 ):
                         record.append(text)
                         first_pass = 1 
                    else:
                         record.append(link)
                except:
                    pass
                #text = each.text
                # record.append(text)
            if ( len(record) > 2 ):
                record.append(state)
                record.append("Lat")
                record.append("Long")             
                records.append(record)

    # print("len records: ", len( records ))
    if ( len(records) < 2 ):
        return df
    df = pd.DataFrame(data=records, columns = columns)
    return df


def get_noaa_stations_locations():
    with open("states_abbr.txt", "r") as fd:
        state_lst = [x.strip().lower() for x in fd.readlines()]

    #print("states:")
    # print(state_lst)
    state_str = "state="
    url = "https://w1.weather.gov/xml/current_obs/seek.php?state=al&Find=Find"
    prior_st = "state=al"

    states_df = pd.DataFrame()
    for st in state_lst:
        my_st = state_str + str(st)
        print("my_st: ", my_st)
        target_url = url.replace(prior_st, my_st)
        print("url: ", target_url )
        df2 = parse_noaa_state_page(target_url, st.upper())
        print(df2.head())
        states_df = states_df.append(df2)
        priot_st = my_st
    return states_df

def get_lat_and_long_for_obs( station):
    xml = get_weather_from_NOAA(station)
    if ( obs_check_xml_data(xml) == False ):
          return ""
    xmld1 = get_data_from_NOAA_xml( xml)
    lat = xmld1[1][7]
    long = xmld1[1][8]
    print(xmld1[1][7])
    print(xmld1[1][8])
    return [ lat, long ]
    
def pass2_gather(csv_file_in, csv_file_out):
    states_df = pd.read_csv(csv_file_in)
    prefix_url = "https://w1.weather.gov/xml/current_obs/"  
    obs_url = "https://w1.weather.gov/xml/current_obs/KAFO.xml"
    lat_and_long = get_lat_and_long_for_obs(obs_url)
   
    print(lat_and_long)
   
    list_of_stations = states_df['XML'].to_list()
    lat_column = []
    long_column = []
   
    for station in list_of_stations:
        print(station)
        target_url =  prefix_url + str(station)
        print( target_url)
        lat_and_long = get_lat_and_long_for_obs(target_url)
        if len(lat_and_long) > 1:
            lat_column.append(lat_and_long[0])
            long_column.append(lat_and_long[1])
       
    print(lat_column)
    print("long:")
    print(long_column)
   
    states_df['latitude'] = lat_column
    states_df['longitude'] = long_column
             
    print("Shape: " , states_df.shape)
    try:
       states_df.to_csv(csv_file_out, mode='w+', index = False)
    except:
       print("error on csv")

if __name__ == "__main__":   
   """ init the app, get args and establish globals """
   parser = argparse.ArgumentParser(description='Construct NOAA station db/csv')
   parser.add_argument('--pass1', help='initialize station csv with station data' )
   parser.add_argument('--pass2', help='Pull in Lat and Log into specified csv' )
   parser.add_argument('--pass2out', help='specifiy a difffernt final out csv')


   args = parser.parse_args()
   
   if (args.pass1):
         states_df = get_noaa_stations_locations()
         print("Shape: " , states_df.shape)
         try:
            states_df.to_csv(str(args.pass1), mode='w+', index = False)
         except:
             print("error on csv")
        
   if ( args.pass2):       
        if (args.pass2out):
            pass2_gather(str(args.pass2), str(args.pass2out))
        else:
            pass2_gather(str(args.pass2), str(args.pass2) )
   
       
   
   #states_df = get_noaa_stations_locations()
   
   # print("Shape: " , states_df.shape)
   # try:
      #  states_df.to_csv("states_db.csv", mode='w+', index = False)
   #except:
   #     print("error on csv")
    
