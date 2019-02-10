import requests
import datetime
import configparser
import argparse
import os
import sys
from datetime import datetime, timedelta
from peewee import *


db = MySQLDatabase(None)    # will be initialized later


class BaseModel(Model):
    """A base model that will use our Sqlite database."""
    class Meta:
        database = db


class SolarEdge(BaseModel):
    ts = DateTimeField()
    ts_epoch = TimestampField(primary_key=True)
    energy = SmallIntegerField()


class MeteoRain(BaseModel):
    ts = DateTimeField()
    ts_epoch = TimestampField(primary_key=True)
    rain_total = FloatField()
    rain_new = FloatField()
    temperature = FloatField()


class MeteoWind(BaseModel):
    ts = DateTimeField()
    ts_epoch = TimestampField(primary_key=True)
    speed = FloatField()
    gust = FloatField()
    direction = CharField()


wind_direction = ['N', 'NNO', 'NO', 'ONO', 'O', 'OSO', 'SO', 'SSO', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']


def config_section_map(conf, section):
    dict1 = {}
    options = conf.options(section)
    for option in options:
        try:
            dict1[option] = conf.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


def parse_args() -> object:
    parser = argparse.ArgumentParser(description='Reads values from multiple API and writes it to MQTT and DB')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='config.rc')
    parser.add_argument('-m', help="get meteo data")
    parser.add_argument('-s', help="get solar edge data")

    return parser.parse_args()


def read_config(conf, config_file):
    try:
        c_mqtt = config_section_map(conf, "MQTT")
    except:
        print("Could not open conf file, or could not find the MQTT conf section in the file")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    try:
        c_db = config_section_map(conf, "DB")
    except:
        print("Could not find the DB conf section")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    try:
        c_alert_sensor = config_section_map(conf, "ALERT_SENSOR")
    except:
        print("Could not find the ALERT_SENSOR conf section")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    try:
        c_solar_edge = config_section_map(conf, "SOLAR_EDGE")
    except:
        print("Could not find the SOLAR_EDGE conf section")
        config_full_path = os.getcwd() + "/" + config_file
        print("Tried to open the conf file: ", config_full_path)
        raise ValueError
    return (c_mqtt, c_db, c_alert_sensor, c_solar_edge)


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
        print("problem contacting the cloud")
        return None
    return response.json()


def api_get_solaredge(conf):
    now = datetime.today()
    now_h1 = now - timedelta(days=0, hours=2)
    headers = {'cache-control': 'no-cache'}
    payload = {"timeUnit": "QUARTER_OF_AN_HOUR", "meters": "Production", "api_key": "ZYKHN7DMQW7HGI8MRGHT0IKN5IVS28XC",
               'endTime': now.strftime('%Y-%m-%d %H:%M:%S'), 'startTime': now_h1.strftime('%Y-%m-%d %H:%M:%S')}
#    payload['startTime'] = '2019-02-01 00:00:00'
#    payload['endTime'] = '2019-03-01 10:00:00'
    url = conf['url']

    try:
        response = requests.request("GET", url, data='', headers=headers, params=payload)
    except:
        print("Could not connect to the SolarEdge Cloud Server. Aborting")
        return None
    if response.status_code == 400:
        print("problem contacting the cloud")
        return None
    if response.status_code == 403:
        print("Error in the datetime arguments")
    return response.json()



def main(conf_mqtt, conf_sensor, conf_solaredge):

    solaredge_json = api_get_solaredge(conf_solaredge)
    if solaredge_json is None:
        exit(1)

    for quarter in solaredge_json['energyDetails']['meters'][0]['values']:
        if 'value' in quarter:
            value = int(quarter['value'])
        else:
            value = 0
        print(quarter['date'], " ", value)
        datetime_object = datetime.strptime(quarter['date'], '%Y-%m-%d %H:%M:%S')
        ep = datetime_object.timestamp()
        ret = SolarEdge.replace(ts=quarter['date'], ts_epoch=ep, energy=value).execute()
    meteo_json = api_get_meteoSensor(conf_sensor)
    if meteo_json is None:
        exit(1)
    for device in meteo_json['devices']:
        measurement = device['measurement']
        id = measurement['idx']
        ts = datetime.fromtimestamp(measurement['ts']).isoformat()
        if 'r' in measurement:
            # rain sensor
            query = MeteoRain.select()
            delta = 0
            if query.exists():
                last_item = MeteoRain.select().order_by(MeteoRain.ts_epoch.desc()).get()
                if int(last_item.ts_epoch.strftime('%s')) != measurement['ts']:
                    delta = measurement['r'] - last_item.rain_total
                    ret = MeteoRain.replace(ts=ts, ts_epoch=measurement['ts'], rain_total=measurement['r'], rain_new=delta,
                                    temperature= measurement['t1']).execute()
            print('Rain ', ts)
        elif 'ws' in measurement:
            # wind sensor
            ret = MeteoWind.replace(ts=ts, ts_epoch=measurement['ts'], speed=measurement['ws'], gust=measurement['wg'],
                                    direction=wind_direction[measurement['wd']]).execute()
            print('Wind ', ts)
    print('data pushed to DB and MQTT ')
    return 0


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    args = parse_args()
    config = configparser.ConfigParser()
    config.read(args.f)
    try:
        (conf_mqtt, conf_db, conf_sensor, conf_solaredge) = read_config(config, args.f)
    except ValueError:
        exit(1)
    db.init(conf_db['db'], host=conf_db['host'], user=conf_db['username'], password=conf_db['password'],
            port=int(conf_db['port']))
    db.connect(conf_db)
#    db.create_tables([SolarEdge, MeteoRain, MeteoWind])

    rtcode = main(conf_mqtt, conf_sensor, conf_solaredge)
    db.close()
    sys.exit(rtcode)
