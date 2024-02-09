from pyspark.sql import SparkSession
import requests
import time

S3_DATA_OUTPUT_PATH = 's3://aviation-poland/output/'

spark = SparkSession.builder.appName('aviationpolandapp').getOrCreate()

# List of all airports in Poland
airports = ['WAW', 'KRK', 'GDN', 'KTW', 'POZ', 'WRO', 'LUZ', 'SZY', 'RZE', 'BZG', 'LCJ', 'SZZ', 'BIA', 'OSR', 'IEG', 'RDO', 'RNB', 'WMI']

# Create an empty DataFrame to store the flight data
flight_df = spark.createDataFrame([], schema="flight_date STRING, airline STRING, flight STRING, flight_status STRING, departure STRING, arrival STRING")

# Define your API key securely
API_KEY = '3ccb587810d549ce14a626b320efdabf'

# Define departures airport
dep_airport = 'WAW'

# Loop through all airport combinations for departure and arrival
for arr_airport in airports:
    # Skip if the departure and arrival airport are the same
    if dep_airport == arr_airport:
        continue
        
    # Define the API request parameters
    params = {
        'access_key': API_KEY,
        'dep_iata': dep_airport,
        'arr_iata': arr_airport
    }

    # Make the API request
    try:
        api_result = requests.get('http://api.aviationstack.com/v1/flights', params=params)
        api_response = api_result.json()
            
        if 'data' in api_response:
            flight_data = [(flight['flight_date'], flight['airline'], flight['flight'], flight['flight_status'], flight['departure'], flight['arrival']) for flight in api_response['data']]
            flight_df = flight_df.union(spark.createDataFrame(flight_data, schema="flight_date STRING, airline STRING, flight STRING, flight_status STRING, departure STRING, arrival STRING"))
        else:
            print(f"API response does not contain flight data for {dep_airport} to {arr_airport}: {api_response}")
            
        # Add a delay to respect rate limits
        time.sleep(1)  # Assuming a rate limit of 1 request per second
    except Exception as e:
        print(f"An error occurred for {dep_airport} to {arr_airport}: {str(e)}")

# Show the first 100 rows of the DataFrame
flight_df.show(100, truncate=False)

# Save DataFrame to CSV file
flight_df = flight_df.repartition(1)  # Set the number of partitions to 1 to generate a single CSV file
flight_df.write.mode('overwrite').option("header", "true").csv(S3_DATA_OUTPUT_PATH + "aviation_data.csv")