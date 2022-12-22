# Project 3: Analysis with Spark

To see the full project spec: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-3.html

## Introduction
This project main goal is to do data and stream analysis using spark. The tech stacks used in this project are: <br>
- Spark <br>
- HDFS <br>
- Python <br>

## Goal
The data used in this project is from 4 - 20 gb of data of subreddit comments. The goal of this project is to show:
### Data Cleaning, Curating, and Collaboration <br>
1. As the title mentioned, this task is about data manipulation and collaboration. Collaboration here means sharing the manipulated data or sharing our approach to get the result on the rest of the questions. <br>

### Site Analysis <br>
2. Subreddit Growth: Show the number of unique subreddits at the start of 2018 and at the end of 2018. <br>
3. User Growth: Determine the number of active users now compared to the past <br>
4. Best Comment Award: Get the most upvoted comment on a particular date <br>
5. Top Comments: Find 5 of the most upvoted comments from the user found on question 4. See if there are any other comments from her/him that get a lot of upvotes or just a "One Hit Wonder" comment. <br>
6. Ban Hammer: Based on user activity, determine which subreddits have been recently banned. <br>

### Text Analysis
7. Screamers: Write a job that can provide a scream score for each subreddits. Scream in here means comments with that are typed in capital letters.
8. Readability: Computes the Gunning Fog Index and Flesch-Kincaid Readability (both reading ease and grade level) of user comments. Then: <br>
-- Choose a subreddit and plot the distribution of these scores.
-- Compare readability of two subreddits of your choosing. Analyze the results.
9. Toxicity: Write a function to perform Sentiment Analysis on comments. Then, compare the toxicity between two subreddits.
10. Targeted Advertising: Find what the user likes/dislikes, where they are from and two other information about the user.

### Stream Analysis
11. Trending Topics: Build a Spark streaming application that will consume the stream and determine what topics are trending over a particular window of time.

### Self-Directed Analysis
12. Clustering related comments on subreddits.
13. Find correlations between multiple subreddits. For example, perhaps users that visit /r/technology also frequently visit /r/android.

## How to Run
1. Have a hdfs cluster running. (https://www.cs.usfca.edu/~mmalensek/cs677/schedule/materials/hadoop-setup.html)
2. Setting up Apache Spark. (https://www.cs.usfca.edu/~mmalensek/cs677/schedule/materials/spark-setup.html)
3. Store the files to be analyzed to hdfs cluster.
4. Start the master and follower nodes.
-- start-all.sh
-- stop-all.sh
5. Run the code in Jupyter Notebook.

