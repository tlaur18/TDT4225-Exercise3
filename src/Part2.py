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


    # 4: Find all users who have taken a taxi.
    def UsersTakenTaxi(self):

        # Fetch activities with taxi as transportation mode
        taxi_activities = [activity['_id'] for activity in self.db['Activity'].find({'transportation_mode': 'taxi'})]

        # Fetch users who created these activities
        taxi_users = list(self.db['User'].find({'activities': {'$in': taxi_activities}}))

        return [(user['_id'],) for user in taxi_users], ("User ID",)


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

        # 4: Find all users who have taken a taxi.
        rows, headers = program.UsersTakenTaxi()
        print(tabulate(rows, headers))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
