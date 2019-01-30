import requests
import datetime
import configparser
import argparse
import os
import sys


url_sensor = "https://www.data199.com/api/pv1/device/lastmeasurement"

payload_sensor = "deviceids=0844F59172CD%2C0B250836A75B&phoneid=789807439177"
headers_sensor = {
    'content-type': "application/x-www-form-urlencoded",
    'cache-control': "no-cache",
    #    'postman-token': "3c48dde2-14ca-413f-6574-6222fa5eb304"
}
headers_sink = {
    'content-type': "application/json",
    'cache-control': "no-cache",
}
es_index='meteosensor'
last_rain = {}
last_wind = {}
id = 0
last_rain['id']=0
last_rain['amount']=0
last_wind['id']=0



def configSectionMap(config, section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def parseTheArgs() -> object:
    parser = argparse.ArgumentParser(description='Reads a value from the BME280 sensor and writes it to MQTT and DB')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='config.rc')

    args = parser.parse_args()
    return args


def readConfig(config):
    try:
        conf_mqtt = configSectionMap(config, "MQTT")
    except:
        print("Could not open config file, or could not find config section in file")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    try:
        conf_db = configSectionMap(config, "DB")
    except:
        print("Could not open config file, or could not find config section in file")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    try:
        conf_alert_sensor = configSectionMap(config, "ALERT_SENSOR")
    except:
        print("Could not open config file, or could not find config section in file")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    return (conf_mqtt, conf_db, conf_alert_sensor)



print("Starting")
def main(cf):

    try:
        (conf_mqtt, conf_db, conf_sensor)=readConfig(cf)
    except ValueError:
        exit(1)
    #    response = requests.request("POST", url_sensor, data=payload_sensor, headers=headers_sensor).json()
    headers = {'cache-control': 'no-cache','content-type': 'application/x-www-form-urlencoded'}
    payload = 'phoneid='+conf_sensor['phoneid']+"&deviceids="+conf_sensor['deviceids']+"&undefined="
    url = conf_sensor['url']
#    payload = "phoneid=789807439177&deviceids=081323E85744%2C0B250836A75B&undefined="

    try:
        response = requests.request("POST", url, data=payload, headers=headers)
    except:
        print("Could not connect to METEO Cloud Server. Aborting")
        exit(1)
    if response.status_code == 400:
        print ("problem contacting the cloud")
        exit(1)
    return_json = response.json()
    for device in return_json['devices']:
        measurement = device['measurement']
        id = measurement['idx']
        m_dict = {}
        m_dict['@timestamp'] = datetime.datetime.fromtimestamp(measurement['ts']).isoformat()
        m_dict['ts_lastseen'] = datetime.datetime.fromtimestamp(measurement['c']).isoformat()
        if 'r' in measurement:
            # rain sensor
            if last_rain['id'] == id:
                #print("rain: same ID, nothing to do")
                #print(m_dict)
                continue
            type = device['deviceid']
            m_dict['total_rain'] = measurement['r']
            m_dict['temperature'] = measurement['t1']
            m_dict['sensor_type'] = 'rain'
            last_rain['id'] = id
            m_dict['additional_rain'] = m_dict['total_rain'] - last_rain['amount']
            last_rain['amount'] = m_dict['total_rain']
        elif 'ws' in measurement:
            # wind sensor
            if last_wind['id'] == id:
                #print("wind: same ID, nothing to do")
                #print(m_dict)
                continue
            type = device['deviceid']
            m_dict['windspeed'] = measurement['ws']
            m_dict['windgust'] = measurement['wg']
            m_dict['winddir'] = measurement['wd']
            last_wind['id'] = id
            m_dict['sensor_type'] = 'wind'
        else :
            type = 'unknown'


        print('data pushed to DB and MQTT ')
        print(m_dict)
    #print("finished.... sleeping")


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    args = parseTheArgs()
    config = configparser.ConfigParser()
    config.read(args.f)

    rtcode = main(config)
    sys.exit(rtcode)
