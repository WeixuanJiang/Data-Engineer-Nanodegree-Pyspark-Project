### Project Overview
Sparkify is a music streaming business that has exploded in popularity in recent months, with its services now being recognised all over the world.

The size of the customer database grew, posing new issues in terms of getting diversified data to business analysts in a timely way. New positions, such as data scientists, will also be created to work with the data.

This project's goal is to integrate the present Data Warehouse into the Big Data world.

### Data Source
Data resides in two directories that contain files in JSON format:

s3a://udacity-dend/song_data : Contains metadata about a song and the artist of that song;
s3a://udacity-dend/log_data : Consists of log files generated by the streaming app based on the songs in the dataset above;

### ETL Pipeline
etl.py: is used for orchestration of the dataset from json into s3 and then running processing jobs with pyspark with following process:
1. Create spark session
2. process song data
3. process log data
4. save all tables to s3