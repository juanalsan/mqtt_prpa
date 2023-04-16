
from paho.mqtt.client import Client
import traceback
import sys
import time 
import random
import numpy as np

def on_connect(mqttc, userdata, flags, rc):
    print("CONNECT:", userdata, flags, rc)

def on_message(client, userdata, msg):
    print(msg.topic, msg.payload)
    try:
        n =  float(msg.payload)
        topics = ["t1", "t2"]
        for topic in topics:
            if msg.topic == 'temperature/' + topic:
                if userdata['maximo_'  + topic] < n:
                    userdata['maximo_' + topic] = n
                if userdata['minimo_'  + topic] > n:
                    userdata['minimo_' + topic] = n
                if userdata['maximo'] < n:
                    userdata['maximo'] = n
                if userdata['minimo'] > n:
                    userdata['minimo'] = n
                userdata['total_' + topic] += n
                userdata['total'] += n
                userdata['numero_datos_' + topic] += 1
                userdata['numero_datos'] += 1
    except ValueError:
        pass
    except Exception as e:
        raise e
        
def on_publish(mqttc, userdata, mid):
    print("PUBLISH:", userdata, mid)

def on_subscribe(mqttc, userdata, mid, granted_qos):
    print("SUBSCRIBED:", userdata, mid, granted_qos)

def on_log(mqttc, userdata, level, string):
    print("LOG", userdata, level, string)

def media (suma_total, num):
    return suma_total/num

  
def main(broker):
    keys = ["_t1", "_t2", ""]
    data = dict()
    for key in keys:
        data["maximo" + key]       = 0
        data["minimo" + key]       = np.inf
        data["total" + key]        = 0
        data["numero_datos" + key] = 0

    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.on_log = on_log

    print(f'Connecting on channels numbers on {broker}')
    mqttc.connect(broker)
    
    mqttc.subscribe('temperature/#')

    mqttc.loop_start()
    t_0 = time.time()

    while True:
        if (time.time()-t_0) > 6: 
            media_t1 = data['total_t1'] / data['numero_datos_t1']
            media_t2 = data['total_t2'] / data['numero_datos_t2']
            media    = str(data['total'] / data['numero_datos'])
            for key in keys:
                print('/clients/maximo' + key
                               , f'{data["maximo" + key]}')
                print('/clients/minimo' + key
                               , f'{data["minimo" + key]}')
                print('/clients/media' + key
                               , f'{media + key}')
            time.sleep(random.random())
            t_0 = time.time()

if __name__ == "__main__":
    hostname = 'simba.fdi.ucm.es'
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
    broker = sys.argv[1]
    main(broker)
