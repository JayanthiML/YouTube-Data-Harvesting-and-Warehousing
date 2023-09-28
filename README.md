# YouTube-Data-Harvesting-and-Warehousing

This is a Project to test the knowledge gained.
I have performed the entire process through python.

The project involves:
  + Data Harvesting from YouTube Data API
  - Storing the Data in MongoDB Atlas Database
  - Migrating the data stored in MongoDB to SQL
  - Displaying answers for some predefined questions using SQL Query
  - Display the entire thing in Streamlit Web Application

Data Harvesting from YouTube Data API:
    - The Google Cloud Console is a web-based interface that allows users to manage and interact with various Google Cloud services and resources.
    - Developers can enable and manage APIs for their projects, as well as access the API Library to discover and enable APIs.
    - https://console.cloud.google.com from this site we should create a project, enable Youtube data API v3 and create API Key.
    - https://developers.google.com/youtube/v3/docs will provide the reference codes on how to extract the required details.
    - The googleapiclient library is a Python client library for interacting with various Google APIs.
    - googleapiclient.discovery.build("youtube", "v3", developerKey=api_key) == It initializes a connection to the YouTube Data API.
    - Copy the code in the reference and use googleapiclient library to extract details.


Storing Data in MongoDB:
  - MongoDB Atlas is a cloud based platform designed to simplify database management for MongoDB, one of the most popular NoSQL databases.
  - Create Account in MongoDB Atlas. Create Cluster, Create Project, Create Database, Create Collections.
  - Set up a connection to a MongoDB database.
  - Store the extracted data in MongoDB for further processing and analysis.

Fetching Data from MongoDB and Creating a DataFrame:
  - Establish a connection to the MongoDB database. 
  - Query the MongoDB collections to retrieve the desired data. 
  - Convert the retrieved data into a pandas DataFrame. 

Migrating Data to SQL for Structuring:
  - Set up a connection to an SQL database (e.g., MySQL, PostgreSQL). 
  - Create the necessary tables to represent the desired data structure. 
  - Migrate the data from the DataFrame to the SQL database, populating the tables with the structured data.

Answering Data Questions using SQL Commands:
  - Using SQL Queries extract required information.
  - Show the required information in structured form.
