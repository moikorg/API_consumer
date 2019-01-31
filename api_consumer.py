import requests
import datetime
import configparser
import argparse
import os
import sys
from datetime import datetime, timedelta

es_index = 'meteosensor'
last_rain = {}
last_wind = {}
id = 0
last_rain['id'] = 0
last_rain['amount'] = 0
last_wind['id'] = 0



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
    parser = argparse.ArgumentParser(description='Reads values from multiple API and writes it to MQTT and DB')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='config.rc')
    parser.add_argument('-m', help="get meteo data")
    parser.add_argument('-s', help="get solar edge data")

    return parser.parse_args()


def readConfig(config):
    try:
        conf_mqtt = configSectionMap(config, "MQTT")
    except:
        print("Could not open config file, or could not find the MQTT config section in the file")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    try:
        conf_db = configSectionMap(config, "DB")
    except:
        print("Could not find the DB config section")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    try:
        conf_alert_sensor = configSectionMap(config, "ALERT_SENSOR")
    except:
        print("Could not find the ALERT_SENSOR config section")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    try:
        conf_solar_edge = configSectionMap(config, "SOLAR_EDGE")
    except:
        print("Could not find the SOLAR_EDGE config section")
        config_full_path = os.getcwd() + "/" + args.f
        print("Tried to open the config file: ", config_full_path)
        raise ValueError
    return (conf_mqtt, conf_db, conf_alert_sensor, conf_solar_edge)


def api_get_meteoSensor(conf):
    headers = {'cache-control': 'no-cache','content-type': 'application/x-www-form-urlencoded'}
    payload = 'phoneid='+conf['phoneid']+"&deviceids="+conf['deviceids']+"&undefined="
    url = conf['url']
    try:
        response = requests.request("POST", url, data=payload, headers=headers)
    except:
        print("Could not connect to METEO Cloud Server. Aborting")
        return None
    if response.status_code == 400:
        print ("problem contacting the cloud")
        return None
    return response.json()


def api_get_solarEdge(conf):
    now = datetime.today() - timedelta(days=180)
    now_h1 = now - timedelta(hours=12)
    headers = {'cache-control': 'no-cache'}
    payload = {"timeUnit": "QUARTER_OF_AN_HOUR", "meters": "Production","api_key": "ZYKHN7DMQW7HGI8MRGHT0IKN5IVS28XC"}
    payload['endTime'] = now.strftime('%Y-%m-%d %H:%M:%S')
    payload['startTime'] = now_h1.strftime('%Y-%m-%d %H:%M:%S')
    url = conf['url']

    try:
        response = requests.request("GET", url, data='', headers=headers, params=payload)
    except:
        print("Could not connect to the SolarEdge Cloud Server. Aborting")
        return None
    if response.status_code == 400:
        print ("problem contacting the cloud")
        return None
    return response.json()


print("Starting")
def main(cf):
    try:
        (conf_mqtt, conf_db, conf_sensor, conf_solaredge)=readConfig(cf)
    except ValueError:
        exit(1)

    solaredge_json = api_get_solarEdge(conf_solaredge)
    if solaredge_json == None:
        exit(1)
    for quarter in solaredge_json['energyDetails']['meters'][0]['values']:
        if 'value' in quarter:
            value = int(quarter['value'])
        else:
            value = 0
        print (quarter['date'], " ", value)

    meteo_json = api_get_meteoSensor(conf_sensor)
    if meteo_json == None:
        exit(1)
    for device in meteo_json['devices']:
        measurement = device['measurement']
        id = measurement['idx']
        m_dict = {}
        m_dict['@timestamp'] = datetime.fromtimestamp(measurement['ts']).isoformat()
        m_dict['ts_lastseen'] = datetime.fromtimestamp(measurement['c']).isoformat()
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
