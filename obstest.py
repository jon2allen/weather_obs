### test loop

def primitive_test_loop():
  trace_print("primitive_test_loop")
  xmlfilename = "KDCA.xml"
  with open(xmlfilename, mode = 'rb') as f:
      content = f.read()
  # print(content)
  tree = ET.fromstring(content)
  header = []
  row = []
  for child in tree:
      print( child.tag, child.attrib)
      header.append(child.tag)
  for child in tree:
      print( child.tag, ":  ", child.text )
      row.append(child.text)
  print(header)
  print( row )     
###############
### replicate same functionality as before 
###############
  rc = weather_csv_driver( 'w', 'KDCA.csv', header, row) 
  print("rc: ( first call )", rc )
  for i in range(0,10):
      rc = weather_csv_driver( 'a', 'KDCA.csv', header, row) 
  rc =  weather_csv_driver( 'w', 'bob_kblw_file.csv', header, row )
  print("rc: ", rc )
  print("#######")
  content = get_weather_from_NOAA('https://w1.weather.gov/xml/current_obs/KJGG.xml')
  print(content)
  xmld1 = get_data_from_NOAA_xml( content)
  weather_csv_driver('w', 'KJGG.csv', xmld1[0], xmld1[1])