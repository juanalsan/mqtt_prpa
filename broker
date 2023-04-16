from paho.mqtt.client import Client

def on_message(client, userdata, msg):
    print(msg.topic, msg.payload)

def main(broker, topic):
    client = Client()
    client.on_message = on_message

    print(f'Connecting on channels {topic} on {broker}')
    client.connect(broker)

    client.subscribe(topic)

    client.loop_forever()

if __name__ == "__main__":
    import sys
    if len(sys.argv)<3:
        print(f"Usage: {sys.argv[0]} broker topic")
    broker = sys.argv[1]
    topic = sys.argv[2]
    main(broker, topic)
