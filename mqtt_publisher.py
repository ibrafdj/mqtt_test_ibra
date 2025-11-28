# This is the main file
import json
import time
import os
import sys
import paho.mqtt.client as mqtt

# Path to the configuration file
CONFIG_PATH = "config.json"

def load_config():
    """Loads configuration from the JSON file."""
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: {CONFIG_PATH} not found.")
        sys.exit(1)
    
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        sys.exit(1)

def main():
    # 1. Load Configuration
    config = load_config()
    
    broker = config.get('broker_ip')
    port = config.get('broker_port', 1883)
    topic = config.get('topic')
    frequency = config.get('publish_frequency', 1)
    message = config.get('data_string', "")

    if not broker or not topic:
        print("Error: 'broker_ip' and 'topic' are required in config.json")
        sys.exit(1)

    print(f"Configuration Loaded: Broker={broker}:{port}, Topic={topic}, Freq={frequency}s")

    # 2. Setup MQTT Client (Using Protocol Version 2 for paho-mqtt v2.x)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    try:
        print(f"Connecting to {broker}...")
        client.connect(broker, port, 60)
        client.loop_start() # Start the network loop in a background thread
        
        while True:
            print(f"Publishing to {topic}: {message}")
            info = client.publish(topic, message)
            info.wait_for_publish() # Ensure delivery
            time.sleep(frequency)
            
    except KeyboardInterrupt:
        print("Disconnecting...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()