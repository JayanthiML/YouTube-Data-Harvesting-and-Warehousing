# YouTube-Data-Harvesting-and-Warehousing

## Problem Statement:
The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

## The application has the following features:
1. Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
2. Option to store the data in a MongoDB database as a collection.
3. Ability to collect data for up to 10 different YouTube channels and store them in MongoDB Database collection.
4. Option to migrate the data from MongoDB Database to a SQL database as tables.
5. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

## Project Overview:
I have performed the entire process through python coding.  
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.
I have written the codes in Modular fashion that is in Functional Blocks.

## The project involves:
  - Data Harvesting from YouTube Data API.
  - Storing the Data in MongoDB.
  - Migrating the data stored in MongoDB to SQL.
  - Displaying answers for some predefined questions using SQL Query.
  - Display the entire thing in Streamlit Web Application

## Python Libraries used:
  - googleapiclient
  - pymongo
  - pandas
  - mysql.connector
  - streamlit

## Skills take away from the project: 
  - Python scripting,
  - Data Collection,
  - MongoDB,
  - Streamlit,
  - API integration,
  - Data Managment using MongoDB (Atlas) and SQL

# Basic Workflow and Execution of the project

## Data Harvesting from YouTube Data API:
  - The Google Cloud Console is a web-based interface that allows users to manage and interact with various Google Cloud services and resources.
  - Developers can enable and manage APIs for their projects, as well as access the API Library to discover and enable APIs.
  - https://console.cloud.google.com from this site we should create a project, enable Youtube data API v3 and create API Key.
  - https://developers.google.com/youtube/v3/docs will provide the reference codes on how to extract the required details.
  - The googleapiclient library is a Python client library for interacting with various Google APIs and to make requests to the API.
  - googleapiclient.discovery.build("youtube", "v3", developerKey=api_key) == It initializes a connection to the YouTube Data API.
  - Copy the code in the reference and use googleapiclient library to extract details.

## Storing Data in MongoDB Atlas Database:
  - MongoDB Atlas is a cloud based platform designed to simplify database management for MongoDB, one of the most popular NoSQL databases.
  - It can handle unstructured and semi-structured data easily.
  - Create Account in MongoDB Atlas. Create Cluster, Create Project, Create Database, Create Collections.
  - Set up a connection to a MongoDB database. Use the connection string and pymongo library to interact with MongoDB.
  - Store the extracted data in MongoDB for further processing and analysis.

## Fetching Data from MongoDB and Creating a DataFrame:
  - Establish a connection to the MongoDB database.
  - Query the MongoDB collections to retrieve the desired data.
  - Convert the retrieved data into a pandas DataFrame. 

## Migrating Data to SQL for Structuring:
  - Set up a connection to an SQL database (e.g., MySQL, PostgreSQL).
  - Create the necessary tables to represent the desired data structure.
  - Migrate the data from the DataFrame to the SQL database, populating the tables with the structured data.

## Answering some predefined Questions related to the data collected using SQL Commands:
  - Using SQL Queries extract required information.
  - Show the required information in structured form.

## Creating and Displaying the data in Web Application:
  - Streamlit is a great choice for building data visualization and analysis tools quickly and easily.
  - You can display the retrieved data in the Streamlit app.
  - Users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.
  - I have created Buttons for each tasks. And buttons will appear after the execution of the prior button.
