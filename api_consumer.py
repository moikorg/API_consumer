import requests
import datetime
import configparser
import argparse
import os
import sys
from datetime import datetime, timedelta
from peewee import *
import paho.mqtt.client as mqtt

db = MySQLDatabase(None)  # will be initialized later


class BaseModel(Model):
    """A base model that will use our Sqlite database."""

    class Meta:
        database = db


class SolarEdge(BaseModel):
    ts = DateTimeField()
    ts_epoch = TimestampField()
    energy = SmallIntegerField()
    id = IntegerField(primary_key=True)


class MeteoRain(BaseModel):
    id = IntegerField(primary_key=True)
    ts = DateTimeField()
    ts_epoch = TimestampField()
    rain_total = FloatField()
    rain_new = FloatField()
    temperature = FloatField()


class MeteoWind(BaseModel):
    id = IntegerField(primary_key=True)
    ts = DateTimeField()
    ts_epoch = TimestampField()
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
    headers = {'cache-control': 'no-cache', 'content-type': 'application/x-www-form-urlencoded'}
    payload = 'phoneid=' + conf['phoneid'] + "&deviceids=" + conf['deviceids'] + "&undefined="
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
#    payload['startTime'] = '2020-04-01 00:00:00'
#    payload['endTime'] = '2020-05-01 10:00:00'
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

    print("Date from solar edge:")
    for quarter in solaredge_json['energyDetails']['meters'][0]['values']:
        if 'value' in quarter:
            value = int(quarter['value'])
        else:
            value = 0
        print(quarter['date'], " ", value)
        datetime_object = datetime.strptime(quarter['date'], '%Y-%m-%d %H:%M:%S')
        ep = int(datetime_object.timestamp())

        found_element_sel = SolarEdge.select().where(SolarEdge.ts_epoch == ep)
        try:
            found_element = found_element_sel.get()
        except:
            SolarEdge.insert(energy=value, ts=datetime_object, ts_epoch=ep).execute()
        else:
            ret = found_element.update(energy=value).where(SolarEdge.ts_epoch == ep).execute()


    meteo_json = api_get_meteoSensor(conf_sensor)
    if meteo_json is None:
        exit(1)

    print("\nData from meteo sensor:")
    for device in meteo_json['devices']:
        print(device)
        if 'measurement' not in device:
            continue
        measurement = device['measurement']
        id = measurement['idx']
        ts = datetime.fromtimestamp(measurement['ts']).isoformat()
        ep = int(measurement['ts'])
        if 'r' in measurement:
            # rain sensor
            query = MeteoRain.select()
            delta = 0
            if query.exists():
                last_item = MeteoRain.select().order_by(MeteoRain.ts_epoch.desc()).get()
                if int(last_item.ts_epoch.strftime('%s')) != ep:
                    delta = measurement['r'] - last_item.rain_total
                    MeteoRain.replace(ts=ts, ts_epoch=ep, rain_total=measurement['r'],
                                            rain_new=delta,
                                            temperature=measurement['t1']).execute()
                    writeRainMQTT(conf_mqtt, ep, measurement['r'], delta, measurement['t1'])
            print('Rain ', ts)
        elif 'ws' in measurement:
            # wind sensor
            found_element_rain_sel = MeteoWind.select().where(MeteoWind.ts_epoch == ep)
            try:
                found_element_rain = found_element_rain_sel.get()
            except:
                MeteoWind.insert(ts=ts, ts_epoch=ep, speed=measurement['ws'], gust=measurement['wg'],
                                    direction=wind_direction[measurement['wd']]).execute()
                writeWindMQTT(conf_mqtt, ep, measurement['ws'], measurement['wg'], wind_direction[measurement['wd']])
            else:
                print("Noting to do, element exists already. Entry with epoch time "+str(ep)+" already exists")

            print('Wind ', ts, ' epoch: ', ep)
    print('\nData pushed to DB and MQTT ')
    return 0


def connectMQTT(conf):
    broker = conf['host']
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.username_pw_set(username=conf['username'],
                           password=conf['password'])
    try:
        client.connect(broker, 1883, 60)
    except:
        print("ERROR: Can not connect to MQTT broker")
        return -1
    return client


def writeRainMQTT(conf, ep, rainTot, rainDelta, temperature):
    print("Write MQTT")
    client = connectMQTT(conf)
    if client == -1:
        return -1

    mqtt_json = "{\"ts\":\"" + str(ep) + "\"," + \
                "\"totalRain\":" + str(rainTot) + "," + \
                "\"deltaRain\":" + str(rainDelta) + "," + \
                "\"temperature\":" + str(temperature) + "}"
    client.publish("sensor/meteo/3", mqtt_json)  # publish
    client.disconnect()


def writeWindMQTT(conf, ep, speed, gust, direction):
    print("Write MQTT")
    client = connectMQTT(conf)
    if client == -1:
        return -1

    mqtt_json = "{\"ts\":\"" + str(ep) + "\"," + \
                "\"speed\":" + str(speed) + "," + \
                "\"gust\":" + str(gust) + "," + \
                "\"direction\":\"" + str(direction) + "\"}"
    client.publish("sensor/meteo/4", mqtt_json)  # publish
    client.disconnect()


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))


def on_publish(client, userdata, result):
    print("Data published")
    pass

def on_disconnect(client, userdata, rc):
    print("disconnecting reason  " + str(rc))


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
