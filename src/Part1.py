import os, re, json
import pandas as pd
from pathlib import Path
from pprint import pprint
from DbConnector import DbConnector


class Part1:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        self.db.create_collection(collection_name)    
        print(f'Created collection: {collection_name}')
        
    def drop_coll(self, collection_name):
        self.db[collection_name].drop()
        print(f'Dropped collection: {collection_name}')

    def insert_documents(self, collection_name, data):
        collection = self.db[collection_name]
        collection.insert_many(data)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents[:5]: 
            pprint(doc)
        
    def show_coll(self):
        collections = self.client['geolife'].list_collection_names()
        print(collections)


    

def walk():

    # The following relative directory structure was used. Change if yours is different
    relative_path = '../../dataset/dataset'
    dataset_path = os.path.join(os.path.dirname(__file__), os.path.realpath(relative_path))

    # Read list of users with transportation labels
    file = open(dataset_path + '/labeled_ids.txt', "r")
    user_ids_with_labels = file.read().split("\n")
    user_ids_with_labels = list(filter(None, user_ids_with_labels)) # Remove empty strings from list

    users, activities, trackpoints = [], [], []
    user_id_regex = r'\b\d{3}\b'
    current_user = None
    current_activity = 1
    trackpoint_id = 1
    for root, dirs, files in os.walk(dataset_path + '/Data'):
        path = root.split(os.sep)

        # Check if we have reached a new user directory
        if (re.match(user_id_regex, os.path.basename(root))):

            user_id = os.path.basename(root)
            has_labels = user_id in user_ids_with_labels
            
            user_dict = {'_id': user_id, 'has_labels': has_labels, 'activities': []}
            users.append(user_dict)

            current_user = user_id
            print('Now reading user ' + current_user)

        # Check if we have entered the trajectory directory of current_user
        if (os.path.basename(root) == 'Trajectory'):

            # Potential to find transportation mode if the user has registered these
            if has_labels:
                labels_file = open(Path(root) / ".." / "labels.txt", "r")
                lines = labels_file.readlines()[1:]
                labels_file.close()
                # Every start and end time (without any formatting) and transportation_mode with same index
                start_datetimes = [line.split("\t")[0].replace("/", "").replace(" ", "").replace(":", "").strip() for line in lines]
                end_datetimes = [line.split("\t")[1].replace("/", "").replace(" ", "").replace(":", "").strip() for line in lines]
                modes = [line.split("\t")[2].strip() for line in lines]

            for file in files:
                file_complete = root + '/' + file
                
                # Skip activities with more than 2500 TrackPoints
                with open(file_complete,'r') as f:
                    if (len(f.readlines()) > 2506):
                        continue

                # Read file to dataframe
                df = pd.read_csv(file_complete, delimiter=',', header=None, names=['lat', 'lon', 'ignore', 'altitude', 'date_days', 'date', 'time'], parse_dates={'date_time': ['date', 'time']}, skiprows=6)
                df = df.drop('ignore', axis=1)
                df['activity_id'] = current_activity
                df = df[['activity_id', 'lat', 'lon', 'altitude', 'date_days', 'date_time']]

                activity_user_id = current_user

                start_date_time = df['date_time'].iloc[0]
                end_date_time = df['date_time'].iloc[-1]

                # Find transportation_mode by comparing start and end datetime of activity with the times in labels.txt
                transportation_mode = None
                if has_labels:
                    start_time_matchable = str((start_date_time)).replace("-", "").replace(" ", "").replace(":", "")
                    end_date_matchable = str(end_date_time).replace("-", "").replace(" ", "").replace(":", "")
                    
                    if (start_time_matchable in start_datetimes) and (end_date_matchable in end_datetimes):
                        # Exact match found
                        transportation_mode = modes[end_datetimes.index(end_date_matchable)]
                
                activity_dict = {'_id': current_activity, 'transportation_mode': transportation_mode, 'start_date_time': start_date_time, 'end_date_time': end_date_time}
                activities.append(activity_dict)
                user_dict['activities'].append(current_activity)
                current_activity += 1

                for activity_id, lat, lon, altitude, date_days, date_time in df.values.tolist():
                    tp_dict = {'_id': trackpoint_id, 'lat': lat, 'lon': lon, 'altitude': altitude, 'date_days': date_days, 'date_time': date_time}
                    trackpoints.append(tp_dict)
                    trackpoint_id += 1

    return users, activities, trackpoints
         


def main():
    program = None
    try:
        program = Part1()

        users, activities, trackpoints = walk()

        # Create collections User, Activity, TrackPoint
        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="TrackPoint")

        # Insert data
        program.insert_documents(collection_name="User", data=users)
        program.insert_documents(collection_name="Activity", data=activities)
        program.insert_documents(collection_name="TrackPoint", data=trackpoints)

        # Fetch data
        program.show_coll()
        program.fetch_documents(collection_name="User")
        program.fetch_documents(collection_name="Activity")
        program.fetch_documents(collection_name="TrackPoint")

        # # Drop collections User, Activity, Trackpoint
        # program.drop_coll(collection_name="User")
        # program.drop_coll(collection_name="Activity")
        # program.drop_coll(collection_name="TrackPoint")
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
