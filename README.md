# YouTube Data Harvesting and Warehousing

##  Project Overview
YouTube Data Harvesting and Warehousing is an end-to-end data pipeline project that collects data from the YouTube Data API, stores it in MongoDB, and then structures it in a SQL database for analysis. It also provides an interactive interface using Streamlit to explore and query the data.

The goal of this project is to demonstrate the ability to build a complete data ingestion and analytics system — from API extraction to visual exploration.

---

## Problem Statement
YouTube channels generate a large volume of data, including channel information, videos, and engagement metrics. However, this data is not readily available in a structured format for analysis.

This project:
- Extracts channel and video data using the YouTube Data API
- Stores raw data in a NoSQL database (MongoDB)
- Migrates cleaned data to a SQL database for structured queries
- Provides a Streamlit interface to explore and visualize the data

---

## Repository Structure

├── BaseFile.py                # Core functions for API extraction & DB operations  
├── Streamlit_App.py           # Streamlit frontend  
└── Requirements.txt           # Python dependencies  

---

## How It Works

### 1. **Data Harvesting (Extract)**
- Use the YouTube Data API v3 to fetch channel and video details by channel ID
- Extract fields like:
  - Channel name
  - Subscriber count
  - Video list
  - View count, likes, comments, etc.

### 2. **Storing Raw Data (MongoDB)**
- Store extracted data in collections inside MongoDB
- Acts as a data lake for semi-structured data

### 3. **Data Transformation and Warehouse (SQL)**
- Retrieve data from MongoDB
- Clean and transform using Python and Pandas
- Insert into a relational database (MySQL / PostgreSQL) for structured analysis

### 4. **Exploration & Querying**
- Use SQL queries to answer questions like:
  - Most viewed videos
  - Highest engagement metrics
  - Subscriber trends
- Results are shown in Streamlit

### 5. **Interactive Visualization (Streamlit)**
- The app takes input (Channel ID)
- Displays structured data
- Shows tables & visual summaries
- Users can navigate and analyze data in a friendly UI

---

## Technologies Used

- **Python:** Core scripting language  
- **YouTube Data API:** For data extraction  
- **MongoDB:** NoSQL storage for raw data  
- **MySQL:** Structured database for analysis  
- **Streamlit:** Interactive frontend  
- **Pandas:** Data processing  
- **googleapiclient:** API integration  
- **pymongo / mysql.connector:** Database connectors

---

## Installation & Setup

1. **Clone the repository**
```
git clone https://github.com/JayanthiML/YouTube-Data-Harvesting-and-Warehousing.git
````

2. **Install dependencies**

```
pip install -r Requirements.txt
```

3. **Set up YouTube API**

* [Create a Google Cloud project](https://console.cloud.google.com)
* Enable YouTube Data API v3
* Obtain your API key
* [Reference Codes](https://developers.google.com/youtube/v3/docs)

4. **Configure databases**

* Set up MongoDB (Atlas)
* Set up SQL database (MySQL)
* Update connection strings in your script or config

5. **Run Streamlit App**

```
streamlit run Streamlit_App.py
```

---

## Usage

* Enter a YouTube **channel ID**
* Click **Harvest Data**

* Fetches and saves data into MongoDB
* Click **Migrate to SQL**

* Transfers data into structured SQL tables
* Visualize analytics in the app interface

---

## Features

✔ API-based data extraction  
✔ MongoDB raw storage  
✔ SQL warehousing  
✔ Querying with SQL joins  
✔ Interactive data exploration using Streamlit

---

## What You Can Do

* Fetch data from multiple channels
* Analyze metrics such as views, likes, comments
* Compare channels
* Build dashboards within Streamlit

---

## Skills and Learnings

* API integration
* NoSQL & SQL data storage
* ETL pipeline building
* Interactive app development
* Data cleaning and transformation
* Data analysis and visualization

---

## Author

**M L Jayanthi**  
Aspiring Data Scientist  
🔗 [https://github.com/JayanthiML](https://github.com/JayanthiML)  
🔗 [https://www.linkedin.com/in/jayanthi-ml/](https://www.linkedin.com/in/jayanthi-ml/)

---
