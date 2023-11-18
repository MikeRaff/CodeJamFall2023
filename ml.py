import gym
from gym import spaces
import numpy as np
import pandas as pd

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


load_df = pd.read_csv("loads.csv")
truck_df = pd.read_csv("trucks.csv")


load_df["As_load_object"] = load_df.apply(series_to_load, axis = 1)
truck_df["As_truck_object"] = load_df.apply(series_to_truck, axis = 1)

load_df["array"] = load_df.apply(series_to_features_load, axis = 1)
truck_df["array"] = load_df.apply(series_to_features_truck, axis = 1)


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
        
        for j in range (len(load_df["Seq"])):
            for i in range (1, 11):
                all_load_features.append(load_df.iloc[j][i])
                
        all_truck_features = []
        
        for j in range (len(truck_df["Seq"])):
            for i in range (1, 8):
                all_truck_features.append(truck_df.iloc[j][i])
        
        self.state = np.array(all_load_features + all_truck_features)

        # Initial state
        self.initial_state = self.state.copy()

    def reset(self):
        self.state = self.initial_state.copy()
        return self.state

    def step(self, action):
        # Update the state based on the selected truck for each load
        for i in range(self.num_loads):
            self.state[i] = self.trucks[action[i]].match_load(self.loads[i])

        # Calculate the reward (you may define your own reward function)
        reward = self.calculate_reward()

        # Check if all trucks are matched
        done = all(self.state[:self.num_trucks] > 0)

        return self.state, reward, done, {}

    def calculate_reward(self):
        # Example: Reward based on the total matching score
        return np.sum(self.state[:self.num_loads])

    def match_load(self, load):
        # Example: Matching score based on features
        return np.dot(self.features, load.features)

# Example usage

env = LoadTruckMatchingEnv(loads=load_df, trucks=truck_df)

# Q-learning or any other RL algorithm can be used here for training

# Training loop
for episode in range(10):
    state = env.reset()
    total_reward = 0

    for step in range(1):
        action = np.random.randint(env.num_trucks, size=env.num_loads)  # Random action for illustration
        next_state, reward, done, _ = env.step(action)

        # Update Q-values or any learning algorithm here

        total_reward += reward

        if done:
            break

    print(f"Episode {episode + 1}, Total Reward: {total_reward}")

env.close()
