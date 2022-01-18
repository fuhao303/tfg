import os
import paho.mqtt.client as mqtt
import configparser
import logging
import logging.handlers as handlers

logger = logging.getLogger('echoes_generator')
logger.setLevel(logging.INFO)

LOCAL_PATH = os.path.dirname(os.path.realpath(__file__))

logHandler = handlers.RotatingFileHandler(os.path.join(LOCAL_PATH, 'contadoresdeestrellas_generator.log'), maxBytes=1000000, backupCount=2)
logHandler.setLevel(logging.INFO)
logger.addHandler(logHandler)

config_ini = configparser.ConfigParser()
config_ini.read(os.path.join(LOCAL_PATH, 'config.py'))

MQTT_TOPIC_STATIONS = "station/echoes/#"
MQTT_TOPIC_SERVER_UP = "server/status/up"




config_ini = configparser.ConfigParser()
config_ini.read(os.path.join(LOCAL_PATH, 'config.py'))
MQTT_HOST = str(config_ini['MQTT']['HOST'])
MQTT_PORT = int(config_ini['MQTT']['PORT'])

CONFIG_FILE = os.path.join(LOCAL_PATH, '.meteor_radio.ini')  # parsePath("$HOME/.meteor_radio.ini")

config = configparser.ConfigParser()

config['STATIONS'] = {}

config.read(CONFIG_FILE)

config['STREAMING'] = {}
# parsePath("$HOME/meteor-files/default-noise.wav")
config['STREAMING']['noise_file_path'] = os.path.join(LOCAL_PATH, 'assets', 'default-noise.wav')
config['STREAMING']['m3u8_folder_path'] = os.path.join(LOCAL_PATH, 'stations')  # parsePath("$HOME/meteor-files")
config['STREAMING']['time'] = '1'

stations_playlist_working = {}


def updateConfigFile():
    logger.info("updateConfigFile")
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)
        
def on_station_message(client, userdata, msg):

    topic = msg.topic.split('/')
    if topic[2] == 'event':
        registerStation(topic[3])
        registerStationEvent(topic[3], json.loads(str(msg.payload)))
        try:
            generateNoiseResources(topic[3], json.loads(str(msg.payload))['peak_lower'])
        except Exception as e:
            logger.error(e)
            pass

    elif topic[2] == 'register':
        registerStation(str(msg.payload))

    else:
        logger.warning(msg.topic + " " + str(msg.payload))
        

def listenStations():
    mqtt_client = mqtt.Client()
    # mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_station_message
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    mqtt_client.subscribe(MQTT_TOPIC_STATIONS)
    mqtt_client.loop_forever()
    
def updateConfigFile():
    logger.info("updateConfigFile")
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)


def registerStation(stationName):
    if stationName not in config['STATIONS']:
        logger.info("Register Station %s" % (stationName))
        config['STATIONS'][stationName] = json.dumps({'register': datetime.utcnow().isoformat()})
        updateConfigFile()
        startStation(stationName)


def registerStationEvent(stationName, data):

    station_info = json.loads(config['STATIONS'][stationName])
    if 'last_event' not in station_info or station_info['last_event'] < data['t'][0]:
        logger.info("Event from Station %s" % (stationName))
        station_info['last_event'] = data['t'][0]
        station_info['last_event_date'] = datetime.fromtimestamp(data['t'][0]).utcnow().isoformat()
        if 'total_events' in station_info:
            station_info['total_events'] = station_info['total_events'] + 1
        else:
            station_info['total_events'] = 1

        config['STATIONS'][stationName] = json.dumps(station_info)
        updateConfigFile()
        addEventToPlaylist(stationName, data)
    else:
        logger.warning("Event from Station %s not valid" % (stationName))
        
def loadStations():
    for (config_key, config_val) in config.items(u'STATIONS'):
        startStation(config_key)        
        
        
        
        
        
        
        
        
        
if __name__ == '__main__':


    try:
        updateConfigFile()
        loadStations()
        listenStations()
    except Exception as e:
        logger.error(e)
        raise        
