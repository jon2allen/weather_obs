import json
import os

def lambda_handler(event, context):
    # TODO implement
    os.environ['TZ'] = 'US/Eastern'
    # print(os.getcwd())
    exec_dir = '/var/task' 
    os.chdir('/mnt/csv')
    # os.chdir('/tmp')
    du = os.popen('df -h').readlines()
    for f in du:
        print("Fs:  ", f)
    print("executing script")
    exec = "python " + exec_dir + "/weather_obs.py"
    station = " --station https://forecast.weather.gov/xml/current_obs/KDCA.xml"
    cmd = exec + station
    os.system(cmd)
    print("after command")
    t = os.listdir()
    for i in t:
        print("file: ", i)
    print('Hello from jontest1')
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
