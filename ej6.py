from paho.mqtt.client import Client
import paho.mqtt.publish as publish
import time, sys, math, random

def on_connect(mqttc, userdata, flags, rc):
    print("CONNECT:", userdata, flags, rc)


def on_message(mqttc, userdata, msg):
    global startTime, runTime, messages
    print("Mensaje:", msg.topic, msg.payload)
    
    if msg.topic == 'numbers':
        value = float(msg.payload)
        messages.append(value)
        if value.is_integer():
            if value%3 == 0:
                mqttc.unsubscribe('numbers')
                startTime = time.time()
                runTime = random.randint(4, 10)  
                mqttc.subscribe('temperature')

    else:
        data = msg.topic.split("/")[-1]
        value = float(msg.payload)
        if data in total_data:
            total_data[data].append(value)
        else:
            total_data[data] = [value]   
        messages.append(value)
        
        currentTime = time.time()
        if (currentTime - startTime) > runTime:
            total()
            mqttc.unsubscribe('temperature')
            mqttc.subscribe('numbers') 
            
def on_publish(mqttc, userdata, mid):
    print("PUBLISH:", userdata, mid)

def on_subscribe(mqttc, userdata, mid, granted_qos):
    print("SUBSCRIBED:", userdata, mid, granted_qos)
            
            
def total():
    for data in total_data:
        minimum, maximum, average = sats(total_data[data])
        print(f"data: {data}, Min: {minimum}, Max: {maximum}, Media: {average}")
    minimum, maximum, average = sats(messages)
    print(f"Recuento , Min: {minimum}, Max: {maximum}, Media: {average}")
    
def sats(values):
    return min(values), max(values), sum(values) / len(values)

def main(hostname):
    mqttc = Client()
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
    messages = []
    total_data = {}
    main(hostname)
