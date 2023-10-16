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


        # 6a: Find the year with the most activities
        # rows, headers = program.YearWithMostActivities()
        # print(tabulate(rows, headers))

        # 6b: Is this also the year with the most recorded hours?
        rows, headers = program.YearWithMostActivities()
        year_most_activities = rows[0][0]
        print(tabulate(rows, headers))
        rows, headers = program.yearWithMostRecordedHours()
        year_most_active_time = rows[0][0]
        print(tabulate(rows, headers))
        print(f"The year with the highest numebr of activities is {year_most_activities}, the year with the most recorded hours or time is {year_most_active_time}")
        print(f"Are they the same year? {year_most_activities == year_most_active_time}")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
