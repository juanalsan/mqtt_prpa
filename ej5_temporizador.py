from paho.mqtt.client import Client
from multiprocessing import Process
from time import sleep
import sys


def timer(client, msg):
    mensaje = msg.payload.split(", ")
    wait, topic, contenido = mensaje[0], mensaje[1], mensaje[2]
    sleep(float(wait))
    client.publish(topic, contenido)

def on_message(client, data, msg):
    try:
        p = Process(target = timer, args=(client, msg)) 
        p.start()
    except Exception as e:
        print(e)

    
def main(hostname):
    data={'status' : 0}
    mkttc = Client(userdata=data)
    mkttc.on_message = on_message
    mkttc.connect(hostname)
    mkttc.subscribe("clients/timer")
    mkttc.publish
    mkttc.loop_forever
    
if __name__ == "__main__":
    hostname = 'simba.fdi.ucm.es'
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    hostname = sys.argv[1]
    main(hostname)
