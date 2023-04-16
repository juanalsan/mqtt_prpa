
from paho.mqtt.client import Client
import traceback
import sys


def on_connect(mqttc, userdata, flags, rc):
    print("CONNECT:", userdata, flags, rc)


def on_message(client, userdata, msg):
    print(msg.topic, msg.payload)
    try:
        numero =  float(msg.payload)
        if numero.is_integer():
            userdata['frec_enteros'] += 1
            client.publish('/clients/frec_enteros', f'{userdata["frec_enteros"]}')
            client.publish('/clients/enteros', numero)
        else:
            userdata['frec_reales'] += 1
            client.publish('/clients/frec_reales', f'{userdata["frec_reales"]}')
            client.publish('/clients/reales', numero)
    except ValueError:
        pass
    except Exception as e:
        raise e

        
def on_publish(mqttc, userdata, mid):
    print("PUBLISH:", userdata, mid)

def on_subscribe(mqttc, userdata, mid, granted_qos):
    print("SUBSCRIBED:", userdata, mid, granted_qos)


def main(hostname):
    data = {
        'frec_enteros': 0,
        'frec_reales':0
    }
    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.connect(hostname)
    
    mqttc.subscribe('numbers')
    mqttc.loop_forever()

if __name__ == '__main__':
    hostname = 'simba.fdi.ucm.es'
    if len(sys.argv)>1:
        hostname = sys.argv[1]

    main(hostname)
