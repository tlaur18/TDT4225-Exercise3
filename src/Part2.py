from DbConnector import DbConnector
from tabulate import tabulate


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
        rows, headers = program.TransportationModeCounts()
        print(tabulate(rows, headers))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
