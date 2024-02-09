<p align="center"><img src="https://www.sbcar.eu/wp-content/uploads/2018/05/Gdansk-University-of-Technology-loggo.png" width="300" align="middle"></p>

# Air Traffic Poland
Academic project at Gdansk University of Technology

## Description
Design and implement an analytical platform to analyze measurement data in almost real-time. The main purpose of this project is to build a solution that allows us to monitor air traffic in Poland.

## Technologies
The project aimed to use cloud technologies to efficiently process large data sets. For this purpose, the Amazon Web Services (AWS) platform was used, on which the experiments were launched with the utilization of a computing cluster. In the context of cloud computing, we defined the following technological requirements:

* Account on https://awsacademy.instructure.com/ ($100 credit available for project purposes)
* Single node EMR cluster on AWS with EMR 7.0.0
* Cluster software configuration: Hadoop 3.3.6, Hive 3.1.3, JupyterEnterpriseGateway 2.6.0, Livy 0.7.1, Spark 3.5.0
* Cluster hardware configuration: m4.xlarge
* Configuration of SSH access to the cluster based on EC2 key pair
* S3 cloud storage


## Dataset
Source dataset: https://aviationstack.com/documentation

The aviationstack API was built to provide a simple way of accessing global aviation data for real-time and historical flights as well as allow customers to tap into an extensive data set of airline routes and other up-to-date aviation-related information.

## The Process
The data for this project was obtained from the Aviationstack API ( https://aviationstack.com/documentation ). The API provides access to real-time flight data for airports around the world, including Poland. 

We used the API to retrieve flight data for the following airports in Poland: Warsaw Chopin Airport (WAW), Gdańsk Lech Wałęsa Airport (GDN), Katowice International Airport (KTW), Poznań Ławica Airport (POZ), and Wrocław–Copernicus Airport (WRO). 

The data provided by the API includes information about the airline, flight number, departure and arrival times, and flight status.

The data is returned in JSON format, which we parsed using the Python programming language and the Requests library. 

We used the Spark DataFrame library to create a data frame from the parsed data, which we then used for our data analysis.

After that we saved the DataFrame as CSV file and we store it using S3 cloud storage

## Results
![image](https://github.com/wajdha/air-traffic-Poland/assets/103352403/4fdd32ab-a9ea-4e65-8518-0526f3e915c0)
![image](https://github.com/wajdha/air-traffic-Poland/assets/103352403/fedec3ec-2aa3-455b-9e14-17eebecdb126)


## Conclusion
The Air Traffic Poland project successfully developed an analytical platform to monitor air traffic in Poland using cloud technologies, specifically leveraging the capabilities of Amazon Web Services (AWS). By integrating the Aviationstack API, the platform obtained real-time flight data for major airports in Poland, enabling comprehensive analysis.


### Project developers:
* Wajd Hatem
* Murad Abdullayev
