import datetime

import pymongo
from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit
from pymongo import GEOSPHERE


class GeolifeQueries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        

    # 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
    def AllTableCounts(self):
        user_count = self.db['User'].count_documents({})
        activity_count = self.db['Activity'].count_documents({})
        tp_count = self.db['TrackPoint'].count_documents({})
        return [('User', user_count), ('Activity', activity_count), ('TrackPoint', tp_count)], ("collection", "count")


    # 2: Find the average number of activities per user.
    def AvgActivitiesPerUser(self):
        user_count = self.db['User'].count_documents({})
        activity_count = self.db['Activity'].count_documents({})
        avg_activities_per_user = activity_count / user_count
        return [(avg_activities_per_user,)], ("AvgActivitiesPerUser",)
    
    
    # 3: Find the top 20 users with the highest number of activities.
    def Top20UsersWithMostActivities(self):
        
        user_collection = self.db['User']

        pipeline = [
            {
                '$unwind': '$activities'  # Unwind the array of activity IDs
            },
            {
                '$group': {
                    '_id': '$_id',  # Group by user ID
                    'totalActivities': {'$sum': 1}  # Count the activities for each user
                }
            },
            {
                '$sort': {'totalActivities': -1}  # Sort in descending order
            },
            {
                '$limit': 20  # Limit the results to the top 20 users
            }
        ]

        result = user_collection.aggregate(pipeline)

        result_list = list(result)

        return [user.values() for user in result_list], ("User ID", "Number of activites")


    # 4: Find all users who have taken a taxi.
    def UsersTakenTaxi(self):

        # Fetch activities with taxi as transportation mode
        taxi_activities = [activity['_id'] for activity in self.db['Activity'].find({'transportation_mode': 'taxi'})]

        # Fetch users who created these activities
        taxi_users = list(self.db['User'].find({'activities': {'$in': taxi_activities}}))

        return [(user['_id'],) for user in taxi_users], ("User ID",)


    # 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
    def TransportationModeCounts(self):

        pipeline = [
            {
                # Filter away activities with None transportation modes
                '$match':
                {
                    'transportation_mode': {'$ne': None}
                }
            },
            {
                # Group by transportation mode and count number of documents in each group
                '$group':
                {
                    '_id': '$transportation_mode',
                    'activity_count': {'$sum': 1}
                }
            },
            {
                # Sort in order of descending activity_count
                '$sort':
                {
                    'activity_count': -1
                }  
            }
        ]

        result = self.db['Activity'].aggregate(pipeline)

        return [r.values() for r in result], ("transportation_mode", "activity_count")




    # 6a: Find the year with the most activities
    # note: we count an activity as belonging to the year it began in. If an activity begins in 2007, but ends in 2008 it still belongs only to 2007.
    def YearWithMostActivities(self):
        
        activities = self.db["Activity"]

        pipeline = [
            {
                '$project': # documents coming from this stage will be on the format {"_id": <id>, "year": <year of start_date_time}
                {
                    'year': {'$year': "$start_date_time"}, 
                }
            },
            {
                '$group': {
                    '_id': '$year',  # Group by year
                    'totalActivities': {'$sum': 1}  # Count the activities for each year
                    
                }
            },
            {
                '$sort': {'totalActivities': -1}  # Sort in descending order
            },
            {
                '$limit': 1  # Limit the results to the top year
            }
           
        ]

        result = list(activities.aggregate(pipeline))

        return [(years["_id"], years["totalActivities"]) for years in result], ("Year", "Number of activites")
    

    # 6b: Is this also the year with the most recorded hours?
    # Note: this methods only counts whole hours. If an activity lasted 4.0, 4.5 or 4.8 it will only be counted as 4 hours
    def yearWithMostRecordedHours(self):

        activities = self.db["Activity"]

        pipeline = [
            {
                '$project': # documents coming from this stage will be on the format {"_id": <id>, "year": <year of start_date_time}, "hours": <number of hours between start and end time of activity}
                {   
                    'year': {'$year': "$start_date_time"},
                    'seconds': {'$dateDiff' : 
                            {
                                'startDate': '$start_date_time',
                                'endDate': '$end_date_time',
                                'unit': 'second'
                            }
                    }
                }        
            },
            {
                '$group': {
                    '_id': '$year',  # Group by year
                    'totalSecondsRecorded': {'$sum': '$seconds'}  # Add up the hours recorded for that year
                    
            },
            },
            {
                '$sort': {'totalSecondsRecorded': -1}  # Sort in descending order
            },
            {
                '$limit': 1 # Limit the results to the top year
            }
        ]

        result = list(activities.aggregate(pipeline))

        return [(years["_id"], years["totalSecondsRecorded"]/3600) for years in result], ("Year", "Number of hours recorded")
        
    # 7: Find the total distance (in km) walked in 2008, by user with id=112
    def DistanceWalkedByUser112In2008(self):

        # Fetch the document of User 112
        user112 = self.db['User'].find_one({'_id': '112'})

        # Extract User 112's activity ids
        activity_ids = user112['activities']

        # From these ids, get activity documents with 'walk' as transportation_mode
        walking_activities = self.db['Activity'].find({'_id': {'$in': activity_ids}, 'transportation_mode': 'walk'})

        total_dist = 0
        for a in walking_activities:
            # Get trackpoints from this activity. Ensure they are sorted chronologically
            trackpoints = self.db['TrackPoint'].find({'_id': {'$in': a['trackpoints']}}).sort('date_time', 1)
            trackpoints = list(trackpoints)

            # Calculate total distance
            for i in range(len(trackpoints)-1):

                # Skip if trackpoints not recorded in 2008
                if trackpoints[i]['date_time'].year != 2008 or trackpoints[i+1]['date_time'].year != 2008:
                    continue
                
                lat1, lon1 = trackpoints[i]['lat'], trackpoints[i]['lon']
                lat2, lon2 = trackpoints[i+1]['lat'], trackpoints[i+1]['lon']
                dist = haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)
                total_dist += dist

        return [(total_dist,)], ("DistanceWalkedByUser112In2008",)
    

    # 8: Find the top 20 users who have gained the most altitude meters.
    def Top20AltitudeGainers(self):

        result = []

        pipeline = [
            {
                # Filter away trackpoints with invalid altitudes
                '$match':
                {
                    'altitude': {'$ne': -777}
                }
            },
            {
                # Sort in order of user_id, activity_id, date_time
                '$sort':
                {
                    'user_id': 1,
                    'activity_id': 1,
                    'date_time': 1
                }  
            },
            {
                # Include only the following relevant fields
                '$project': {'user_id': 1, 'activity_id': 1, 'altitude': 1}
            }
        ]

        trackpoints = self.db['TrackPoint'].aggregate(pipeline)
        trackpoints = list(trackpoints)

        current_user = None
        current_activity = None
        last_altitude = None
        cumulative_altitude_gain = 0
        for tp in trackpoints:

            # If next trackpoint is from new user
            if tp['user_id'] != current_user:
                # Save result for user
                result.append((current_user, cumulative_altitude_gain))

                # Reset cumulative altitude gain for next user
                cumulative_altitude_gain = 0

                # Set current_user and current_activity and skip to next iteration
                current_user = tp['user_id']
                current_activity = tp['activity_id']
                last_altitude = tp['altitude']
                continue

            # If next trackpoint is from new activity of the same user
            if tp['activity_id'] != current_activity:
                # Set current_user and current_activity and skip to next iteration
                current_activity = tp['activity_id']
                last_altitude = tp['altitude']
                continue
                
            if tp['altitude'] > last_altitude:
                cumulative_altitude_gain += (tp['altitude'] - last_altitude) / 3.281
                last_altitude = tp['altitude']
        
        # Sort result by altitude gain descending
        result = sorted(result, key=lambda tup: tup[1], reverse=True)

        return result[:20], ("id", "total_meters_gained")

    # 9: Find all users who have invalid activities, and the number of invalid activities per user
    def UsersWithInvalidActivities(self):

        trackpoints = self.db["TrackPoint"].find({},{"user_id":1, "activity_id": 1, "date_time" : 1 })


        current_user = None
        current_activity = None
        last_date_time = None
        invalidated = False
        
        result= {}
        added = False

        for object in trackpoints: 
            vals = list(object.values())
            *_, date_time, user_id, activity_id = vals
            
         
            if user_id != current_user:
                # New user reached, also new activity
                print("now checking user ", user_id)
                result[user_id] = 0
                current_user = user_id
                current_activity = None
                invalidated = False
                added = False

            if activity_id == current_activity and invalidated:
                if not added:
                    result[user_id] += 1
                    
                    added = True
                continue

            if activity_id != current_activity:
                # new activity reached, this needs to be treated as the previous datetime in the next iteration
                # if activity is already invalidated, skip until we reach the next activity or user
                current_activity = activity_id
                last_date_time = date_time
                invalidated = False
                added = False
                continue
            
            if date_time:
                date_time = datetime.datetime.strptime(str(date_time),"%Y-%m-%d %H:%M:%S" )
                last_date_time= datetime.datetime.strptime(str(last_date_time),"%Y-%m-%d %H:%M:%S")
                time_diff = date_time - last_date_time
                minutes_diff = minutes_diff = divmod(time_diff.total_seconds(), 60)[0]
            
            
                if minutes_diff >= 5.0:
                    invalidated = True
        
          
        not_in = [x for x,y in result.items() if y == 0]
        result = {x:y for x, y in result.items() if y != 0} # remove users with 0 invalid activities
       
        print(not_in)
        return list(result.items()), ('user_id', '# of invalid activities')
    

    # 10: Find the users who have tracked an activity in the Forbidden City of Beijing.
    # Note: Since there was zero tracpoints with the exact lat and lon given in the task, 
    #  we search insted for the trackpoints that deviate by 0.0005 in either direction in either lat or lon
    # 0.0005 was chosen since the coordinates were given with a precision of 0.01
    def UsersVisitedForbiddenCityNaive(self):
        forbidden_lat = 39.916
        forbidden_lon = 116.397
        
        pipeline = [
            {
                "$match": {
                    "lat":
                        { "$gte": forbidden_lat - 0.0005, "$lte": forbidden_lat + 0.0005},
                    "lon":
                        { "$gte": forbidden_lon - 0.0005, "$lte": forbidden_lon + 0.0005}     
                },
                
            },
            {
                "$project":{
                    "user_id": 1,
                    "lat": 1,
                    "lon": 1
                }
            }
        ]

        user_set = set()
        for doc in self.db["TrackPoint"].aggregate(pipeline):
            vals = list(doc.values())
            *_, lat, lon, user_id = vals
            user_set.add(user_id)

        return [(user, ) for user in user_set], ("User that visited Forbidden City of Bejing",)
    
    
    # 10: Find the users who have tracked an activity in the Forbidden City of Beijing.
    # Note: this method was an attempt at a more sophisticated approach to answering the query,
    # by using mogodb's geospatial queries. This would allow for making a query that finds all trackpoints within
    # a given radius of the location provided. This requires creating a new "2dSphere" index.
    # However, we were unsucsessful in creating this query as it seemed to take to long and never finished creating the index
    # both when creating it programmatically with pymongo but also when doing it directly in mongosh    
    def UsersVisitedForbiddenCity(self):
        forbidden_lon = 116.397
        forbidden_lat = 39.916



        # this piece of code was ran once to create another collection which conformed to the pattern needed to make geoqueries
        # pipeline = [
        #     {
        #         "$project": {
        #             "location": {
        #                 "type": "Point",
        #                 "coordinates": ["$lat", "$lon"]
        #             },
                    
        #             "user_id": 1,
        #             "activity_id": 1

        #         }
        #     },
        #     { 
        #         "$out": "TrackPointGeo" 
                
        #     }
        # ]
        # trackpoints = self.db["TrackPoint"].aggregate(pipeline)

        trackpoints_geo = self.db["TrackPointGeo"]

        print(trackpoints_geo)

        #This line creates a new index to enable geo spatial queries, but it took to long to execute.
        #trackpoints_geo.create_index([("location", GEOSPHERE)] )

        # if the index had been created correctly this query would find all trackpoints which were a 100 meters or closer to the given point
        close_tps = trackpoints_geo.find(
            {
                "location":
                {
                    "$near":
                    {
                        "$geometry": {"type": "Point",  "coordinates": [ forbidden_lon, forbidden_lat ] },
                        "$minDistance": 100,
                    }
                }
            }
        )

        return "didn't work"

    # 11: Find all users who have registered transportation_mode and their most used transportation_mode.
    def UsersWithTransportationModes(self):

        result = []
        
        # Get all User documents
        users = self.db["User"].find({})

        for user in users:
            user_id, has_labels, activity_ids = user['_id'], user['has_labels'], user['activities']

            # Skip if user has no registered transportation modes
            if not has_labels:
                continue

            pipeline = [
                {
                    # Filter by particular User's activities and discard activities with None transportation_mode
                    '$match':
                    {
                        '_id': {'$in': activity_ids},
                        'transportation_mode': {'$ne': None}
                    }
                },
                {
                    # Group by transportation mode and count number of documents in each group
                    '$group':
                    {
                        '_id': '$transportation_mode',
                        'activity_count': {'$sum': 1}
                    }
                },
                {
                    # Sort in order of descending activity_count, then by '_id' (transportation_mode)
                    '$sort':
                    {
                        'activity_count': -1,
                        '_id': 1,
                    }  
                },
                {
                    # Limit to get the top transportation mode for each user
                    '$limit': 1
                }
            ]

            most_used_transport = list(self.db['Activity'].aggregate(pipeline))
            if most_used_transport:
                result.append((user_id, most_used_transport[0]['_id']))

        return result, ("user_id", "most_used_transportation_mode")


def main():
    program = None
    try:

        program = GeolifeQueries()

        # 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        rows, headers = program.AllTableCounts()
        print(tabulate(rows, headers))

        # 2: Find the average number of activities per user.
        rows, headers = program.AvgActivitiesPerUser()
        print(tabulate(rows, headers))

        # 3: Find the top 20 users with the highest number of activities.
        rows,headers = program.Top20UsersWithMostActivities()
        print(tabulate(rows, headers))

        # 4: Find all users who have taken a taxi.
        rows, headers = program.UsersTakenTaxi()
        print(tabulate(rows, headers))

        # 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
        rows, headers = program.TransportationModeCounts()
        print(tabulate(rows, headers))

        # 6a: Find the year with the most activities
        rows, headers = program.YearWithMostActivities()
        print(tabulate(rows, headers))

        # 6b: Is this also the year with the most recorded hours?
        rows, headers = program.YearWithMostActivities()
        year_most_activities = rows[0][0]
        print(tabulate(rows, headers))
        print()
        rows, headers = program.yearWithMostRecordedHours()
        year_most_active_time = rows[0][0]
        print(tabulate(rows, headers))
        print()
        print(f"The year with the highest number of activities is {year_most_activities}, the year with the most recorded hours or time is {year_most_active_time}")
        print(f"Are they the same year? {year_most_activities == year_most_active_time}")

        # 7: Find the total distance (in km) walked in 2008, by user with id=112
        rows, headers = program.DistanceWalkedByUser112In2008()
        print(tabulate(rows, headers))
            
        # 8: Find the top 20 users who have gained the most altitude meters.
        rows, headers = program.Top20AltitudeGainers()
        print(tabulate(rows, headers))

        # 9: Find all users who have invalid activities, and the number of invalid activities per user
        rows, headers = program.UsersWithInvalidActivities()
        print(tabulate(rows, headers))

        # Naive 10: Find the users who have tracked an activity in the Forbidden City of Beijing.
        rows, headers= program.UsersVisitedForbiddenCityNaive()
        print(tabulate(rows, headers))

        # Smarter? 10: Find the users who have tracked an activity in the Forbidden City of Beijing.
        # Note: couldn't get this working, creating the index took way too long
        # this code wont run
        # result = UsersVisitedForbiddenCity()
        # print(result)

        # 11: Find all users who have registered transportation_mode and their most used transportation_mode.
        rows, headers = program.UsersWithTransportationModes()
        print(tabulate(rows, headers))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
