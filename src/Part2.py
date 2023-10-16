from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit


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


def main():
    program = None
    try:

        program = GeolifeQueries()

        # 1: How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        # rows, headers = program.AllTableCounts()
        # print(tabulate(rows, headers))

        # 2: Find the average number of activities per user.
        # rows, headers = program.AvgActivitiesPerUser()
        # print(tabulate(rows, headers))

        # 3: Find the top 20 users with the highest number of activities.
        # rows,headers = program.Top20UsersWithMostActivities()
        # print(tabulate(rows, headers))

        # 4: Find all users who have taken a taxi.
        # rows, headers = program.UsersTakenTaxi()
        # print(tabulate(rows, headers))

        # 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
        # rows, headers = program.TransportationModeCounts()
        # print(tabulate(rows, headers))


        # 6a: Find the year with the most activities
        # rows, headers = program.YearWithMostActivities()
        # print(tabulate(rows, headers))

        # 6b: Is this also the year with the most recorded hours?
        # rows, headers = program.YearWithMostActivities()
        # year_most_activities = rows[0][0]
        # print(tabulate(rows, headers))
        # print()
        # rows, headers = program.yearWithMostRecordedHours()
        # year_most_active_time = rows[0][0]
        # print(tabulate(rows, headers))
        # print()
        # print(f"The year with the highest numebr of activities is {year_most_activities}, the year with the most recorded hours or time is {year_most_active_time}")
        # print(f"Are they the same year? {year_most_activities == year_most_active_time}")

        # 7: Find the total distance (in km) walked in 2008, by user with id=112
        # rows, headers = program.DistanceWalkedByUser112In2008()
        # print(tabulate(rows, headers))

        # TODO: Investigate the slight difference between these results and the ones from exercise 2. Maybe it has something to do with the altitudes sometimes having double values
        # 8: Find the top 20 users who have gained the most altitude meters.
        # rows, headers = program.Top20AltitudeGainers()
        # print(tabulate(rows, headers))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
