This assessment involves engineering a data ingest process for ball-by-ball data from professional cricket matches. The data include the overall match results and outcomes for every delivery for both teams. The questions below are an opportunity to showcase your data engineering skills as they involve building a data ingest pipeline and running exploratory analysis on that data set. 


There are many implementation options and the assessment provides an opportunity for you to determine what assumptions are appropriate and operate within those. Since the purpose of this assessment is to showcase your technical and problem-solving skills, please include clear, efficient, and well-organized code along with explanations on the justification for your approach. Please also include instructions on how to run your code, and structure it in a way that makes it as reproducible as possible. 


This assessment is expected to take approximately 3-6 hours, though you do not need to complete it in one sitting. We would like for you to return your work to us within 2 days of receiving the assessment. If an alternative schedule has been arranged with you personally, please follow the agreed-upon schedule. 

Data Set Description 
In this assessment, you will be working with One Day International (ODI) match results and ball-by-ball innings data. These data were sourced from cricsheet.org and include ball-by-ball summaries of ODIs from 2006-present for men and 2009-present, for women. 


Both data sets are in JSON format, which have been compiled from the source YAML files on cricsheet. A full description of the source data structure and definition of variables is available here. 


The basic rules of ODI cricket can be found here. We list the key rules that will be the most useful context for the assessment below. For an ODI, 


1. Each team plays one innings (yes, ‘innings’ is singular) consisting of 50 overs, with 6 deliveries per over. The team who bats first is determined by a coin toss. 


2. A ‘win’ is recorded when one side scores more runs than the opposing side and all the innings of the team that has fewer runs have been completed. The side scoring more runs has ‘won’ the game, and the side scoring fewer has ‘lost’. If the match ends without all the innings being completed, the result may be a tie or no result. 


3. There is theoretically no limit to the number of runs that can be earned off a single delivery as the run tally can increase as the striker and non-striker run to opposite ends of the pitch. However, a hit that bounces and reaches the boundary is an automatic 4 runs and a hit that hits or passes the 1 boundary without a bounce is an automatic 6 runs. 

4. A ‘wicket’ is cricket’s equivalent to an out in baseball. A batter continues batting until they are out. The main ways to take a wicket (or ‘dismiss’ an opposing player) are for the bowler to dismiss a batter with the delivery (e.g. bowled out, leg before wicket, etc.), to catch a batted ball on the fly, or to throw out either the batter or the non-striker as they attempt to run between the wickets. 


5. Each team starts with 10 wickets. Once all wickets are lost the innings ends, whether the 50 overs have been completed or not. 


If you still feel like you need more grounding in the game of cricket, you can take 17min of the assessment time to watch Netflix’s Explained: Cricket which can be watched for free on Youtube.


Assessment Questions 

Please complete the coding portion of the assessment using Python and present your final results with all required code artifacts to run the solution in a zip file. Questions 0,3,4 and 5 include written responses; please include a PDF with your answers. The final submission will include both the zip file with your code and PDF with written answers or you can provide Github URL for the project with all required files.


#Question 0. We don’t expect you to have any cricket knowledge and that is not a requirement to ace this assessment. But we understand that familiarity with cricket may vary from one candidate to the next so we would like to know how you would rate your knowledge of cricket from 1 to 5, where 1 is basically no knowledge (like you had never seen or read anything about the sport until the days before this assessment) and 5 is highly knowledgeable (you watch matches regularly and have a jersey for the Rajasthan Royals in your closet, for example). 
"I would rate my knowledge of cricket as a 2-3. I have had very limited exposure to the sport and have little to no prior knowledge of cricket. I am starting with a very basic understanding of the sport and its rules."

Question 1. Develop a batch data ingest process to load the ODI match results and ball-by-ball innings data to a database of your choosing (as a default, you can use SQLite). The solution should include a step that downloads files directly from cricsheet.org and performs any required preprocessing. The database schema should store match results and ball-by-ball innings data along with the universe of players that appear across all matches. The process should be runnable from the command line, inclusive of creating any dependencies (e.g. local file directories, the database, etc.). Please include a README.md file with instructions on how to build and run the ingest process to reproduce your results. 
->  import requests
# Download match results data
match_results_url = "https://cricsheet.org/downloads/odi_match_list.csv"
match_results_data = requests.get(match_results_url).text

with open("odi_match_results.csv", "w") as file:
    file.write(match_results_data)

innings_data_url = "https://cricsheet.org/downloads/sample/odi_ball_by_ball_data.zip"
innings_data = requests.get(innings_data_url)

with open("odi_ball_by_ball_data.zip", "wb") as file:
    file.write(innings_data.content)


import sqlite3

conn = sqlite3.connect("cricket_data.db")
cursor = conn.cursor()

# Create tables for match results
cursor.execute("""
    CREATE TABLE IF NOT EXISTS MatchResults (
        match_id INTEGER PRIMARY KEY,
        match_date TEXT,
        winner TEXT,
        venue TEXT
    )
""")

# Create tables for innings data
cursor.execute("""
    CREATE TABLE IF NOT EXISTS InningsData (
        match_id INTEGER,
        innings_number INTEGER,
        player_name TEXT,
        runs_scored INTEGER,
        wickets_taken INTEGER,
        FOREIGN KEY (match_id) REFERENCES MatchResults (match_id)
    )
""")

# Create a table for players
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Players (
        player_name TEXT PRIMARY KEY
    )
""")

conn.commit()
conn.close()


import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect("cricket_data.db")
cursor = conn.cursor()

# Load and insert match results data
match_results_df = pd.read_csv("odi_match_results.csv")
match_results_df.to_sql("MatchResults", conn, if_exists="replace", index=False)

# Load and insert innings data (Sample code, adjust as needed)
innings_data_df = pd.read_csv("odi_ball_by_ball_data.zip", compression="zip")
innings_data_df.to_sql("InningsData", conn, if_exists="replace", index=False)

# Extract and insert unique player names
players = innings_data_df["player_name"].unique()
players_df = pd.DataFrame(players, columns=["player_name"])
players_df.to_sql("Players", conn, if_exists="replace", index=False)

conn.close()


Question 2. Using the database populated in Question 1, develop queries to answer the questions below. Please include the queries as .sql files with your code submission. a. The win records (percentage win and total wins) for each team by year and gender, excluding ties, matches with no result, and matches decided by the DLS method in the event that, for whatever reason, the planned innings can’t be completed. b. Which male and female teams had the highest win percentages in 2019? c. Which players had the highest strike rate as batsmen in 2019? (Note to receive full credit, you need to account for handling extras properly.) 
#a. The win records (percentage win and total wins) for each team by year and gender, excluding ties, matches with no result, and matches decided by the DLS method:

WITH MatchStatus AS (
    SELECT
        m.match_id,
        m.match_date,
        m.gender,
        m.innings,
        m.winner
    FROM MatchResults m
    WHERE m.winner IS NOT NULL
        AND m.innings IS NOT NULL
        AND m.innings != '0'
        AND m.innings != 'DL'
)
SELECT
    EXTRACT(YEAR FROM match_date) AS year,
    gender,
    team,
    COUNT(*) AS total_wins,
    (COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM match_date), gender), 0)) * 100 AS win_percentage
FROM MatchStatus
GROUP BY year, gender, team
ORDER BY year, gender, team;


#b. Which male and female teams had the highest win percentages in 2019?
-> 
    WITH MatchStatus AS (
        SELECT
            m.match_id,
            m.match_date,
            m.gender,
            m.innings,
            m.winner
        FROM MatchResults m
        WHERE m.winner IS NOT NULL
            AND m.innings IS NOT NULL
            AND m.innings != '0'
            AND m.innings != 'DL'
    )
    SELECT
        EXTRACT(YEAR FROM match_date) AS year,
        gender,
        team,
        COUNT(*) AS total_wins,
        (COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM match_date), gender), 0)) * 100 AS win_percentage
    FROM MatchStatus
    GROUP BY year, gender, team
)
SELECT
    year,
    gender,
    team,
    win_percentage
FROM WinRecords
WHERE year = 2019
ORDER BY gender, win_percentage DESC
LIMIT 2;

#c. Which players had the highest strike rate as batsmen in 2019? (Accounting for extras properly)

WITH BattingStats AS (
    SELECT
        p.player_name,
        p.gender,
        i.match_date,
        i.innings_number,
        SUM(i.runs_scored) AS total_runs,
        SUM(i.balls_faced) AS total_balls_faced,
        SUM(i.extras) AS total_extras
    FROM InningsData i
    JOIN Players p ON i.player_name = p.player_name
    WHERE EXTRACT(YEAR FROM i.match_date) = 2019
    GROUP BY p.player_name, p.gender, i.match_date, i.innings_number
)
SELECT
    EXTRACT(YEAR FROM match_date) AS year,
    gender,
    player_name,
    (total_runs / NULLIF(total_balls_faced - total_extras, 0)) * 100 AS strike_rate
FROM BattingStats
ORDER BY year, gender, strike_rate DESC
LIMIT 1;



Question 3. Please provide a brief written answer to the following question. The coding assessment focused on a batch backfilling use case. If the use case was extended to required incrementally loading new match data on a go-forward basis, how would your solution change?
If the use case were extended to require incrementally loading new match data on an ongoing basis, several changes and considerations would come into play:

Change in Data Source: The initial solution focused on downloading a full dataset from cricsheet.org. For incremental loading, we would need to establish a mechanism to access and fetch only the new or updated data. This could involve using data APIs, web scraping, or any other method that provides access to new match data as it becomes available.

Incremental Data Processing: Instead of processing the entire dataset from scratch, the solution would need to identify and process only the new data. This might involve tracking the latest available data in the database and fetching and processing data from that point onward. This requires efficient data deduplication and validation processes.

Data Timestamping: Timestamps should be associated with each record in the database to indicate when the data was added. This is crucial for tracking and identifying new data and ensuring that updates are correctly reflected.

Scheduled Execution: The solution would need a scheduling mechanism to run the data ingestion process at regular intervals. Whether it's daily, weekly, or another frequency, scheduling ensures that the system automatically fetches and processes new data.

Data Validation: Robust data validation and integrity checks become even more critical during incremental loading. The solution should be able to detect and handle data anomalies or errors that might occur in the new data.

Logging and Monitoring: To ensure the reliability of the process, it's important to implement logging and monitoring. This allows us to track the status of the data ingestion process, receive notifications of any issues, and take corrective actions promptly.

Backup and Recovery: Data backup and recovery procedures should be in place to address potential data loss or corruption during the incremental loading process. This is essential for safeguarding data integrity.

Automated Script: The solution would need an automated script or application that can be run as a scheduled task. This script would be responsible for periodically fetching, preprocessing, and loading new data into the database without manual intervention.

Efficiency and Performance: As this process becomes ongoing, optimization for efficiency and performance is crucial. Parallel processing and other performance improvements may be necessary to handle larger volumes of data efficiently.

Schema Evolution: The data schema or structure may evolve over time with new data sources or requirements. The solution should be designed to handle schema changes gracefully, accommodating new fields or modifications to existing ones.

Data Deduplication: Deduplication mechanisms should be in place to avoid inserting duplicate records in the database. This ensures data consistency and prevents redundancy.

Security: The security and access controls of the solution should remain robust, especially as it operates continuously. This includes protection against unauthorized access, data breaches, and data leakage.

Scalability: The solution should be scalable to accommodate the growing volume of new data over time. This may involve considerations for database performance and infrastructure scaling.



Question 4. Can you provide an example of when, during a project or analysis, you learned about (or created) a new technique, method, or tool that you hadn’t known about previously? What inspired you to learn about this and how were you able to apply it?
In a data analysis project that involved processing a large CSV dataset in Databricks, I encountered a situation where I needed to perform complex operations on date and time data within the CSV file. The dataset included timestamps, and 
I needed to calculate time intervals, aggregate data by specific time periods, and identify trends over time. This prompted me to learn about the Databricks function window() and the use of time-based aggregations.
also i have tasked with performing data profiling and quality assessment on a large CSV dataset. The dataset contained multiple columns, and you needed to identify missing values, outliers, and anomalies in the data.

What Inspired Me to Learn:
The project required a deep analysis of time-related data, including tracking changes and patterns over specific time intervals. This required a more advanced approach than basic date-time manipulations.

 I applied it :
Time-Based Aggregations:
I used the window() function to perform time-based aggregations. This allowed me to group data into specific time intervals (e.g., hours, days, or weeks) and calculate aggregates within those intervals.

Time Series Analysis:
I applied time series analysis techniques to identify trends and patterns in the data over time. This involved using rolling windows and lag functions to compare data at different time points.

Data Profiling:
I used Databricks to perform data profiling, which involved generating summary statistics for each column, including mean, median, standard deviation, and data distribution.

Handling Missing Values:
I used Databricks to identify missing values in the dataset and employed techniques like imputation, removal, or flagging of missing data, depending on the specific analysis requirements.

Data Validation and Cleansing:
I implemented data validation checks to identify and correct data anomalies, such as inconsistent date formats, duplicate records, and data integrity issues.

Data Quality Reports:
I generated data quality reports that summarized the findings, including the percentage of missing data, the number of outliers, and data cleansing actions taken.

