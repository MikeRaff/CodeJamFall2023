import paho.mqtt.client as mqtt
import pandas as pd

# Create dataframes for loads and trucks
load_df = pd.DataFrame(columns=["Seq", "Timestamp", "LoadID", "OriginLatitude", "OriginLongitude", "DestinationLatitude", "DestinationLongitude", "EquipmentType", "Price", "Mileage"])
truck_df = pd.DataFrame(columns=["Seq", "Timestamp", "TruckID", "PositionLatitude", "PositionLongitude", "EquipType", "NextTripLengthPreference"])

# Simulation has begun
is_started = False

def process_start_event(message):
    is_started = True
    
    # Erasing all data from the DataFrame (keeping columns)
    load_df.drop(load_df.index, inplace=True)
    truck_df.drop(load_df.index, inplace=True)
        
    


def process_load_event(message):
    # Extracting load information from the message
    seq = message.get("seq", None)
    timestamp = message.get("timestamp", None)
    load_id = message.get("loadId", None)
    origin_latitude = message.get("originLatitude", None)
    origin_longitude = message.get("originLongitude", None)
    destination_latitude = message.get("destinationLatitude", None)
    destination_longitude = message.get("destinationLongitude", None)
    equipment_type = message.get("equipmentType", None)
    price = message.get("price", None)
    mileage = message.get("mileage", None)
    
    ### Need to compute load distance using start and end coordinates and match this with trip_length_preferences

def process_truck_event(message):
    # Extracting truck information from the message
    seq = message.get("seq", None)
    timestamp = message.get("timestamp", None)
    truck_id = message.get("truckId", None)
    position_latitude = message.get("positionLatitude", None)
    position_longitude = message.get("positionLongitude", None)
    equip_type = message.get("equipType", None)
    next_trip_length_preference = message.get("nextTripLengthPreference", None)


def process_end_event(message):
    is_started = False
    
    ### Compile results or something

def process_message(message):
    
    message_type = message.get("type", None)

    if message_type == "Start":
        process_start_event(message)
    elif message_type == "Load":
        process_load_event(message)
    elif message_type == "Truck":
        process_truck_event(message)
    elif message_type == "End":
        process_end_event(message)
    else:
        raise ValueError(f"Unknown message type: {message_type}")
        

        
        


############ Connection Stuff ##############


# MQTT Connection Info
host = "fortuitous-welder.cloudmqtt.com"
port = 1883
user = "CodeJamUser"
password = "123CodeJam"
clean_session = True
qos = 1
client_id = "<Syntax_Sorcers>03"
topic = "CodeJam"

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribe to the topic
    client.subscribe(topic)

# Callback when a message is received from the server
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Received message on topic '{msg.topic}': {payload}")

    # Process the message
    process_message(payload)    

# Create an MQTT client instance
client = mqtt.Client(client_id=client_id, clean_session=clean_session)

# Set the username and password for the connection
client.username_pw_set(username=user, password=password)

# Set the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(host, port, keepalive=60)

# Start the network loop to handle messages
client.loop_forever()




