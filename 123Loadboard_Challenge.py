import paho.mqtt.client as mqtt
import pandas as pd
import json
from datetime import datetime, timedelta, timezone
from math import radians, sin, cos, sqrt, atan2
import requests

# Create dataframes for loads and trucks
load_df = pd.DataFrame(columns=["Seq", "Timestamp", "LoadID", "OriginLatitude", "OriginLongitude", "DestinationLatitude", "DestinationLongitude", "EquipmentType", "Price", "Mileage"])
truck_df = pd.DataFrame(columns=["Seq", "Timestamp", "TruckID", "PositionLatitude", "PositionLongitude", "EquipType", "NextTripLengthPreference"])
notification_df = pd.DataFrame(columns=["TruckID", "LoadID", "TimestampOfNotication", "EquipType", "EstimatedProfit", "Mileage"])
pending_notifications_df = pd.DataFrame(columns=["TruckID", "LoadID", "Timestamp", "EquipType", "EstimatedProfit", "Mileage"])
    
temp_load_df = pd.DataFrame(columns=["Seq", "Timestamp", "LoadID", "OriginLatitude", "OriginLongitude", "DestinationLatitude", "DestinationLongitude", "EquipmentType", "Price", "Mileage"])
temp_truck_df = pd.DataFrame(columns=["Seq", "Timestamp", "TruckID", "PositionLatitude", "PositionLongitude", "EquipType", "NextTripLengthPreference"])


# Variables
fuel_cost_per_mile = 1.38 # dollars per mile
last_sent = "0" # Timestamp of last sendout push
time_between = 500 # Time between notifications in seconds
is_started = False #  Simulation has begun
process_messages = True

def sort_by_estimated_profit(noty_df):
    sorted_df = notification_df.sort_values(by='EstimatedProfit', ascending=False)
    return sorted_df
        
def straight_line_distance_in_miles(long1, lat1, long2, lat2):
    # Function uses Haversine formula
    # Radius of the Earth in miles
    R = 3958.8
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, long1, lat2, long2])
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c
    return distance

# Returns whether or a not a truck/load pair is compatible
def is_compatible(truck,load):    
    # setting up compatibility variables
    truck_id = truck['TruckID']
    # equipment type
    truck_equip_type = truck['EquipType']
    load_equip_type = load['EquipmentType']
        
    # distance preference
    truck_distance_preference = truck['NextTripLengthPreference']
        
    # Mileage
    load_mileage = load['Mileage']
    if (load_mileage > 200.0):
        load_distance_preference = "Short"
    else:
        load_distance_preference = "Long"
        
    # radius
    truck_long = truck['PositionLongitude']
    truck_lat = truck['PositionLatitude']
    load_long = load['OriginLongitude']
    load_lat = load['OriginLatitude']
    distance = straight_line_distance_in_miles(truck_long, truck_lat, load_long, load_lat)
    
    # we could include a deadhead cutoff but it already affects profit.
            
    if ((truck_equip_type == load_equip_type) and (truck_distance_preference == load_distance_preference) and (distance <100) ):
        return True
    else:
        return False
    
    # print("LOOK HERE BROTHER trucks")
    # print(truck)
    # print(load)
        
    # setting up compatibility variables
    truck_id = truck['TruckID']
    # equipment type
    truck_equip_type = truck['EquipType']
    load_equip_type = load['EquipmentType']
        
    # distance preference
    truck_distance_preference = truck['NextTripLengthPreference']
        
    # Mileage
    load_mileage = load['Mileage']

        
    if (load_mileage > 200.0):
        load_distance_preference = "Short"
    else:
        load_distance_preference = "Long"
        
    # print("Lookie HERE 2 for trucks")
    # print(truck_id)
    # print(truck_equip_type)
    # print(load_equip_type)
    # print(load_mileage)
        
    if ((truck_equip_type == load_equip_type) and (truck_distance_preference == load_distance_preference) ):
        return True
    else:
        return Fals    

def time_elapsed(timestamp):
    global last_sent
    if (last_sent == "0"):
        last_sent = timestamp
    #  Convert timestamps to datetime objects with timezone information
    timestamp1 = datetime.fromisoformat(last_sent)
    timestamp2 = datetime.fromisoformat(timestamp)
    # Convert timestamps to a common timezone (UTC in this case)
    common_timezone = timezone.utc
    timestamp1 = timestamp1.astimezone(common_timezone)
    timestamp2 = timestamp2.astimezone(common_timezone)
    # Calculate time difference
    time_difference = timestamp2 - timestamp1
    # Extract total seconds
    elapsed_seconds = time_difference.total_seconds()
    return elapsed_seconds
    

# Our alogrithm
def compute_profit(price, mileage, first_load):
    return_home_weight = 0
    # the weight takes into account: 
        # the frequency of loads that bring us home for a profit
        # in other words, it prevents us from having to cut our profits by not having to return home with a bad load.
    # this looks at the frequency of loads that would bring us home
    for load_index, second_load_row in load_df.iterrows():         
        # look through the loads
        # compute the distance between the destination and our current position 
        # if its less than 100, add one to the weight
            # computation: straight line distance between the second destination and first origin
        first_origin_long = first_load["OriginLongitude"]
        first_origin_lat = first_load["OriginLatitude"]
        second_deliv_long = second_load_row["DestinationLongitude"]
        second_deliv_lat = second_load_row ["DestinationLatitude"]
        second_delivery_radius = straight_line_distance_in_miles(first_origin_long, first_origin_lat, second_deliv_long, second_deliv_lat)
        if (second_delivery_radius < 100):
            return_home_weight+=1
        # also compute the distance between our destination and the source
        # if its less than 100, add one to the weight
            # computation: straight line distance between our first destination and the second origin
        first_dest_long = first_load["DestinationLongitude"]
        first_dest_lat = first_load["DestinationLatitude"]
        second_origin_long = second_load_row["OriginLongitude"]
        second_origin_lat = second_load_row ["OriginLatitude"]
        first_delivery_radius = straight_line_distance_in_miles(first_dest_long, first_dest_lat, second_origin_long, second_origin_lat)
        if (first_delivery_radius < 100):
            return_home_weight+=1
            
    # assuming the deadhead function produces a mileage
    estimated_profit = price - (mileage * fuel_cost_per_mile) + return_home_weight*20 # - (deadhead * fuel_cost_per_mile)
    return estimated_profit
    
def check_notification_existence(input_df, pending_notifications):
    input_tuple = tuple(input_df.values.flatten())
    pending_or_actual_notifications_tuples = [tuple(row) for row in pending_notifications.values]

    return input_tuple in pending_or_actual_notifications_tuples


                    
def update_notifications():
    print("ADDING pending_notifications to sent_notifications")

    global pending_notifications_df
    global notification_df
    print(pending_notifications_df)
    notification_df = pd.concat([notification_df, pending_notifications_df], ignore_index=True)
    #pending_notifications_df.drop(pending_notifications_df.index, inplace=True) # clears the pending dataframes rows
    pending_notifications_df = pd.DataFrame(columns=["TruckID", "LoadID", "Timestamp", "EquipType", "EstimatedProfit", "Mileage"])
    print("Notifications")
    print(notification_df)
    sorted_notification_df = sort_by_estimated_profit(notification_df)
    print("Sorted Notifications")
    print(sorted_notification_df)
    
    
    # ideally we'd like to organize the loads to optimize profit.
    # so, once a load is considered compatible
    # we sort them based on profit
    # 
    
def remove_row(notification_df, truck_id, load_id, timestamp, equip_type, estimated_profit, mileage):
    print("")
    # row_to_remove = ((notification_df['TruckID'] == truck_id) &
    #                  (notification_df['LoadID'] == load_id) &
    #                  (notification_df['TimestampOfNotification'] == timestamp) &
    #                  (notification_df['EquipType'] == equip_type) &
    #                  (notification_df['EstimatedProfit'] == estimated_profit) &
    #                  (notification_df['Mileage'] == mileage))
    
    # notification_df = notification_df[~row_to_remove]
    # notification_df.reset_index(drop=True, inplace=True)  # Reset index after removing row
    # return notification_df

    
def check_notification_existence(truck_id, load_id, noty_df):
    if noty_df.empty:
        return False
    else:
        for noty_index, noty_row in noty_df.iterrows():
            truck_ID = noty_row['TruckID']
            load_ID = noty_row['LoadID']
            if(truck_ID == truck_id and load_ID == load_id):
                return True
        return False



def new_load_notification(new_load_df):
    if truck_df.empty:
        print("No trucks exist yet")
    else:
        for truck_index, truck_row in truck_df.iterrows():
            truck_ID = truck_row['TruckID']
            load_ID = new_load_df['LoadID'] 
            
            # check if noty already exists
            global pending_notifications_df
            pending_notification_exists = check_notification_existence(truck_ID,load_ID, pending_notifications_df)
            global notification_df
            notification_exists = check_notification_existence(truck_ID,load_ID, notification_df)
            
            # if notification_exists:
            #     # remove it
            #     print("")
            
            # If the notification already exists:
            if (notification_exists == True or pending_notification_exists == True): # then we wont add a notification, unless the price changed...
                # if the notification already exists, then we should delete it and make a new one
    
                print("notification already exists, no need for duplicates")
                return
            else: # else its a new load and we can add it to pending noties
                # create a pending notification
                if (is_compatible(truck_row, new_load_df) == True):
                    if all(v is not None for v in [truck_row['TruckID'], new_load_df['LoadID'], new_load_df['Timestamp'], new_load_df['EquipmentType'], new_load_df['Mileage']]):
    
                        # Creating a DataFrame for the new notification
                        truck_ID = truck_row['TruckID']
                        load_ID = new_load_df['LoadID']
                        timestamp = new_load_df['Timestamp']
                        equipment_type = new_load_df['EquipmentType']
                        estimated_profit = compute_profit(new_load_df['Price'], new_load_df['Mileage'], new_load_df)
                        mileage = new_load_df['Mileage']
                        
                        
                        notification_event_df = pd.DataFrame({
                            "TruckID": [truck_ID],
                            "LoadID": [load_ID],
                            "Timestamp": [timestamp],
                            "EquipType": [equipment_type],
                            "EstimatedProfit": [estimated_profit],
                            "Mileage": [mileage]
                        })
                        # Concatenating the new load event notification with the existing notifications
                        pending_notifications_df = pd.concat([pending_notifications_df, notification_event_df], ignore_index=True)
                        
                        print("Load has been matched to at least one truck")
                        #print(pending_notifications_df)
                    else:
                        print("Invalid Pending notification event data or Notification already exists")
                # else:
                #     print("Load did not get a match")
                # return
        
def new_truck_notification(new_truck_df):
    if load_df.empty:
        print("No loads exist yet")
    else:
        for load_index, load_row in load_df.iterrows():
            truck_ID = new_truck_df['TruckID']
            load_ID = load_row['LoadID']
        
            # check if noty already exists
            global pending_notifications_df
            pending_notification_exists = check_notification_existence(truck_ID,load_ID, pending_notifications_df)
            global notification_df
            notification_exists = check_notification_existence(truck_ID,load_ID, notification_df)
        
            # If the notification already exists:
            if (notification_exists == True or pending_notification_exists == True): # then we wont add a notification, unless the price changed...
                print("notification already exists, no need for duplicates")
                return
            else: # else its a new load and we can add it to pending noties
                # create a pending notification
                if (is_compatible(new_truck_df, load_row) == True):
                    if all(v is not None for v in [new_truck_df['TruckID'], load_row['LoadID'], load_row['Timestamp'], load_row['EquipmentType'], load_row['Mileage']]):
                        # Creating a DataFrame for the new notification
                        truck_ID = new_truck_df['TruckID']
                        load_ID = load_row['LoadID']
                        timestamp = load_row['Timestamp']
                        equipment_type = load_row['EquipmentType']
                        estimated_profit = compute_profit(load_row['Price'], load_row['Mileage'], load_row)
                        mileage = load_row['Mileage']
                                            
                        notification_event_df = pd.DataFrame({
                            "TruckID": [truck_ID],
                            "LoadID": [load_ID],
                            "Timestamp": [timestamp],
                            "EquipType": [equipment_type],
                            "EstimatedProfit": [estimated_profit],
                            "Mileage": [mileage]
                        })
                                            
                        # Concatenating the new load event notification with the existing notifications
                        pending_notifications_df = pd.concat([pending_notifications_df, notification_event_df], ignore_index=True)
                        
                        print("Truck has been matched to at least one load")
                       # print(pending_notifications_df)
                    else:
                        print("Invalid Pending notification event data or Notification already exists")



############ Helper functions are above this line ##############

    
def process_start_event(message):
    is_started = True
    
    # Erasing all data from the DataFrame (keeping columns)
    load_df.drop(load_df.index, inplace=True)
    truck_df.drop(load_df.index, inplace=True)
    
    # Setting start timestamp
    timestamp = message.get("timestamp", None)
    global last_sent
    last_sent = timestamp

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
    
   # Checking if the required information is present
    if all(v is not None for v in [seq, timestamp, load_id, origin_latitude, origin_longitude, destination_latitude, destination_longitude, equipment_type, price, mileage]):
        # Creating a DataFrame for the current load event
        load_event_df = pd.DataFrame({
            "Seq": [seq],
            "Timestamp": [timestamp],
            "LoadID": [load_id],
            "OriginLatitude": [origin_latitude],
            "OriginLongitude": [origin_longitude],
            "DestinationLatitude": [destination_latitude],
            "DestinationLongitude": [destination_longitude],
            "EquipmentType": [equipment_type],
            "Price": [price],
            "Mileage": [mileage]
        })
        # Concatenating the current load event DataFrame with the existing load DataFrame
        global load_df
        load_df = pd.concat([load_df, load_event_df], ignore_index=True)
        
        newest_row = load_df.iloc[-1]
        print("Load event processed successfully")
        new_load_notification(newest_row) 
        #new_load_notification(load_event_df) # notify trucks that theres a new load
    else:
        print("Invalid load event data")
    
    # update the notifications with the pending # this should maybe go somewhere else? also does the time elapsed reset
    global time_between
    if (time_elapsed(timestamp) >= time_between):
        update_notifications()
        time_between += time_elapsed(timestamp)
        

def process_truck_event(message):
    # Extracting truck information from the message
    seq = message.get("seq", None)
    timestamp = message.get("timestamp", None)
    truck_id = message.get("truckId", None)
    position_latitude = message.get("positionLatitude", None)
    position_longitude = message.get("positionLongitude", None)
    equip_type = message.get("equipType", None)
    next_trip_length_preference = message.get("nextTripLengthPreference", None)
    
    # Checking if the required information is present
    if all(v is not None for v in [seq, timestamp, truck_id, position_latitude, position_longitude, equip_type, next_trip_length_preference]):
        # Creating a DataFrame for the current truck event
        
        truck_event_df = pd.DataFrame({
            "Seq": [seq],
            "Timestamp": [timestamp],
            "TruckID": [truck_id],
            "PositionLatitude": [position_latitude],
            "PositionLongitude": [position_longitude],
            "EquipType": [equip_type],
            "NextTripLengthPreference": [next_trip_length_preference]
        })

        # Concatenating the current truck event DataFrame with the existing truck DataFrame
        global truck_df
        truck_df = pd.concat([truck_df, truck_event_df], ignore_index=True)
        
        newest_row = truck_df.iloc[-1]
        print("Truck event processed successfully")
        new_truck_notification(newest_row) # notify loads that theres a new truck
    else:
        print("Invalid truck event data")
    global time_between
    if (time_elapsed(timestamp) >= time_between):
        update_notifications()
        time_between += time_elapsed(timestamp)
        


def process_end_event(message):
    is_started = False
    

    ### Compile results or something

def process_message(message_type, message):
    if process_messages:
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
    else:
        print("Message processing stopped.")
        
        
############ Connection Stuff ##############


# MQTT Connection Info
host = "fortuitous-welder.cloudmqtt.com"
port = 1883
user = "CodeJamUser"
password = "123CodeJam"
clean_session = True
qos = 1
client_id = "<Syntax_Sorcerers>04"
topic = "CodeJam"

# Callback when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribe to the topic
    client.subscribe(topic)

# Callback when a message is received from the server
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    # print(f"Received message on topic '{msg.topic}': {payload}")

    # Process the message
    message_dictionnary = json.loads(payload)
    message_type = message_dictionnary.get("type", None)
    
    if is_started and message_type == "End":
        process_messages = False
        process_end_event(message_dictionnary)
        print("Stopping message processing...")
        # Unsubscribe from the topic to stop receiving messages
        client.unsubscribe(topic)
        print(f"Unsubscribed from topic: {topic}")
    
    process_message(message_type, message_dictionnary)

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




