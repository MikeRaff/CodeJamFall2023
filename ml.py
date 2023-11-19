import gym
from gym import spaces
import numpy as np
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta, timezone
import random

class Load:
    def __init__(self, features):
        self.features = features
        self.seq = features[0]
        self.time_stamp = features[1]
        self.load_id = features[2]
        self.origin_latitude = features[3]
        self.origin_longitude = features[4]
        self.destination_latitude = features[5]
        self.destination_longitude = features[6]
        self.equipment_type = features[7]
        self.price = features[8]
        self.mileage = features[9]
        

class Truck:
    def __init__(self, features):
        self.features = features
        self.seq = features[0]
        self.time_stamp = features[1]
        self.truck_id = features[2]
        self.position_latitude = features[3]
        self.position_longitude = features[4]
        self.equip_type = features[5]
        self.next_trip_length_preference = features[6]

def series_to_load (series):
    features = []
    for i in range (1, 11):
        features.append(series[i])
        
    return Load(features)

def series_to_truck (series):
    features = []
    for i in range (1, 8):
        features.append(series[i])
        
    return Truck(features)


def series_to_features_load (series):
    features = []
    for i in range (1, 11):
        features.append(series[i])
        
    return (features)

def series_to_features_truck (series):
    features = []
    for i in range (1, 8):
        features.append(series[i])
        
    return (features)

def match_load(truck, load):
    score = 0
    if is_compatible(truck, load):
        score += 2
    if time_difference (truck.time_stamp, load.time_stamp) < 600:
        score += 1
        
    if straight_line_distance_in_miles(truck.position_longitude, truck.position_latitude, load.origin_longitude, load.origin_latitude) < 10:
        score += 2
        
    if compute_profit(load.price, load.mileage) > 50:
        score += 2
    return score

load_df = pd.read_csv("loads.csv")
truck_df = pd.read_csv("trucks.csv")



load_df["array"] = load_df.apply(series_to_features_load, axis = 1)
truck_df["array"] = truck_df.apply(series_to_features_truck, axis = 1)

loads = []
trucks = []

for i in range (len(load_df["array"])):
    loads.append(Load(load_df["array"][i]))

for i in range(len(truck_df["array"])):
    trucks.append(Truck(truck_df["array"][i]))
    

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


def is_compatible(truck, load):    
    # setting up compatibility variables
    # equipment type
    # distance preference
    if (load.mileage > 200):
        load_distance_preference = "Short"
    else:
        load_distance_preference = "Long"
    # radius

    distance = straight_line_distance_in_miles(truck.position_longitude, truck.position_latitude, load.origin_longitude, load.origin_latitude)
    
    if ((truck.equip_type == load.equipment_type) and (truck.next_trip_length_preference == load_distance_preference) and (distance < 100)):
        return True
    else:
        return False
def time_difference(timestamp_truck, timestamp_load):
    #  Convert timestamps to datetime objects with timezone information
    timestamp1 = datetime.fromisoformat(timestamp_truck)
    timestamp2 = datetime.fromisoformat(timestamp_load)
    # Convert timestamps to a common timezone (UTC in this case)
    common_timezone = timezone.utc
    timestamp1 = timestamp1.astimezone(common_timezone)
    timestamp2 = timestamp2.astimezone(common_timezone)
    # Calculate time difference
    time_difference = timestamp2 - timestamp1
    # Extract total seconds
    time_difference = time_difference.total_seconds()
    return time_difference
    
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

def compute_profit(price, mileage):
    estimated_profit = price - (mileage * 1.38)  
    # missing deadhead consideration
    return estimated_profit

class LoadTruckMatchingEnv(gym.Env):
    def __init__(self, loads, trucks):
        super(LoadTruckMatchingEnv, self).__init__()

        self.loads = loads
        self.trucks = trucks
        self.num_loads = len(loads)
        self.num_trucks = len(trucks)

        # Action space: truck selection for each load
        self.action_space = spaces.Discrete(self.num_trucks)

        # Observation space: load and truck properties
        self.observation_space = spaces.Box(low=0, high=1, shape=(self.num_loads + self.num_trucks,), dtype=np.float32)

        # State representation: concatenation of load and truck properties
        all_load_features = []
        for j in range(len(load_df["Seq"])):
            for i in range(1, 11):
                if i == 2:
                    date_string =load_df.iloc[j][i]
                    date_format = '%Y-%m-%dT%H:%M:%S'
                    datetime_object = datetime.strptime(date_string, date_format)
                    all_load_features.append(int(datetime_object.timestamp()))
                    
                else:
                    if load_df.iloc[j][i] == "Van":
                        load_df.iloc[j][i] = 1;
                    elif load_df.iloc[j][i] == "Reefer":
                        load_df.iloc[j][i] = 2;
                    elif load_df.iloc[j][i] == "Flatbed":
                        load_df.iloc[j][i] = 3;
                    
                    else:
                        if load_df.iloc[j][i] != -1:
                            as_int = int(load_df.iloc[j][i])
                            all_load_features.append(as_int)
                        else:
                            all_load_features.append(-1)

        all_truck_features = []
        for j in range(len(truck_df["Seq"])):
            for i in range(1, 8):
                
                if i == 2:
                    date_string =truck_df.iloc[j][i]
                    date_format = '%Y-%m-%dT%H:%M:%S'
                    datetime_object = datetime.strptime(date_string, date_format)
                    all_truck_features.append(int(datetime_object.timestamp()))
                else:
                    if truck_df.iloc[j][i] == "Van":
                        truck_df.iloc[j][i] = 1;
                    elif truck_df.iloc[j][i] == "Reefer":
                        truck_df.iloc[j][i] = 2;
                    elif truck_df.iloc[j][i] == "Flatbed":
                        truck_df.iloc[j][i] = 3;
                        
                        
                    elif truck_df.iloc[j][i] == "Short":
                        truck_df.iloc[j][i] = 4;
                        
                    elif truck_df.iloc[j][i] == "Long":
                        truck_df.iloc[j][i] = 5;
                    else:
                        if truck_df.iloc[j][i] != -1:
                            as_int = int(truck_df.iloc[j][i])
                            all_truck_features.append(as_int)
                        else:
                            all_truck_features.append(-1)

        self.state = np.array(all_load_features + all_truck_features)

        # Initial state
        self.initial_state = self.state.copy()

    def reset(self):
        self.state = self.initial_state.copy()
        return self.state
        
        
    def calculate_reward(self):
        reward = 0
        
        for truck in trucks:
            for load in loads:
                if truck != None and load != None:
                    reward += match_load(truck, load)
        return reward

    def step(self, action_index):  # action is a load?
        a = self.state[action_index]
        print(type(a))
        
        unique_id = action_index.load_id

        # Find the best matching truck for the given load
        best_truck = None
        best_score = -1
        index = 0

        for i in range(len(trucks)):
            if best_truck is not None:
                score = match_load(trucks[i], action)
                if score > best_score:
                    best_score = score
                    best_truck = trucks[i]
                    index = i

        if best_truck is not None:
            # Update the position of the best matching truck
            best_truck.position_longitude = action.origin_longitude
            best_truck.position_latitude = action.origin_latitude

        trucks[index] = best_truck

        # Mark the load as matched
        for i in range(len(self.state)):
            if self.state[i] == unique_id:
                for j in range(-2, 8):
                    self.state[j] = -1

        # Calculate the reward
        reward = self.calculate_reward()

        # Check if all trucks are matched
        numeric_state = pd.to_numeric(self.state[:self.num_trucks], errors='coerce')
        done = all(numeric_state > 0)

        return self.state, reward, done
    


# creating environment

class QLearningAgent:
    def __init__(self, state_size, action_size, learning_rate=0.1, discount_factor=0.9, exploration_rate=1.0, exploration_decay=0.995):
        self.state_size = state_size
        self.action_size = action_size
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.exploration_rate = exploration_rate
        self.exploration_decay = exploration_decay
        self.q_table = np.zeros((state_size, action_size))

    def select_action(self, state):
        if np.random.rand() < self.exploration_rate:
            return np.random.choice(self.action_size)
        else:
            return np.argmax(self.q_table[state, :])

    def update_q_table(self, state, action, reward, next_state):
        
        for i in range(len(next_state)):
            if np.issubdtype(type(next_state[i]), np.datetime64):
                next_state[i] = int(datetime.timestamp(next_state[i]))
                print(next_state[i])
        
        best_next_action = np.argmax(self.q_table)
        
        target = reward + self.discount_factor * self.q_table[next_state.astype(int), best_next_action]
        self.q_table[state, action] = (1 - self.learning_rate) * self.q_table[state, action] + self.learning_rate * target
        # Decay exploration rate
        self.exploration_rate *= self.exploration_decay
        
        
env = LoadTruckMatchingEnv(loads=loads, trucks=trucks)
state_size = len(env.state)
action_size = len(env.state)
agent = QLearningAgent(state_size=state_size, action_size=action_size)

total_reward = 0

for episode in range(10):
    state = env.reset()

    for step in range(1):
        action_index = agent.select_action(state)
        print(action_index)
        action = env.state[action_index] #not getting a load object
        next_state, reward, done = env.step(action)

        # Update Q-values
        agent.update_q_table(state, action_index, reward, next_state)

        total_reward += reward

        if done:
            break

    print(f"Episode {episode + 1}, Total Reward: {total_reward}")

env.close()
