import pandas as pd
import requests
from bs4 import BeautifulSoup

def parse_noaa_state_page(url):
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
                columns.append(each.text)
                # print("col: ", str(each.text) )
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
            records.append(record)

    # print("len records: ", len( records ))
    if ( len(records) < 2 ):
        return df
    df = pd.DataFrame(data=records, columns = columns)
    return df

with open("states_abbr.txt", "r") as fd:
    state_lst = [x.strip().lower() for x in fd.readlines()]

print("states:")
print(state_lst)
state_str = "state="
url = "https://w1.weather.gov/xml/current_obs/seek.php?state=al&Find=Find"
prior_st = "state=al"
for st in state_lst:
    my_st = state_str + str(st)
    print("my_st: ", my_st)
    target_url = url.replace(prior_st, my_st)
    print("url: ", target_url )
    df2 = parse_noaa_state_page(target_url)
    print(df2.head())
    priot_st = my_st
    




df = parse_noaa_state_page(url)
print(df.head())