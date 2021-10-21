# Map Reduce on Log Data
###Description : Design and Implement Map-Reduce computational model on log data and perform analysis.

## Overview
As part of this project, a MapReduce program is created for the parallel processing of a synthetically generated log data. The data contains records for computations at discrete time-steps
with different types of logs (like ERROR, DEBUG, INFO and WARN). Multiple map/reduce jobs have been defined to extract various insights from the data.

The map/reduce jobs created are :

1) Distribution of Log Data i.e. distribution of different types of messages across time intervals.
2) Time intervals sorted in descending order with most number of ERROR logs.
3) Quantifying and aggregating generated log messages for each type.
4) Number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the log message.

##Prerequisites and Installation

- Setup Hadoop environment on target system. Depending on the type of OS please refer official documentaion of [Apache Hadoop](http://hadoop.apache.org/) or various blogs on Google.


- Generate executable jar file

   - Clone this repository

   - Open root folder of the project in the terminal and assemble the project jar using command:
   ```sbt clean compile assembly```
   - This command compiles the source code, executes the test cases and builds the executable jar file 
     "LogFileGenerator-assembly-0.1.jar" in the "target/scala-3.0.2/" folder


- Setup Hadoop environment

  - Navigate to the Hadoop folder where you installed the files

  - Execute the following command. This will start the dfs and yarn daemons
  
      ``` start-all.cmd```
  - Create directory in HDFS to store the input file:
  
     ```hdfs dfs -mkdir input```
  - Move the log file into the input folder
  - Create directory in HDFS to store the output file:
     ```hdfs dfs -mkdir output```
  

- Execute the jar file:

    - run the command:
        hadoop jar %Path to your jar% input/LogFileGenerator.2021-10-18.log output/
    - The output will be generated and stored in the output folder with the task name in front of it
    - The main output folder ‘output’ needs to be deleted if repeating any map/reduce job or else an error is raised. Delete folder using:
      ```hdfs dfs -rm -r output_hw2```
  

- Stop Hadoop services 

    - Stop all daemons after execution is completed using:
      ``` 
      stop-yarn.sh
      stop-dfs.sh
      ```
##Map Reduce Tasks
    
###Task 1

- Mapper Class: ```TimeTypeMapper``` 
    - creates a key with time interval and type of log and value of 1
- Reducer Class: ```TimeTypeReducer```
    - reduces the mapper output to key: timeinterval_logtype and value:total occurences
  
###Task 2

- Mapper Class1 : ```ErrorMapper```
    - creates time interval keys which have "ERROR" log type with value 1
- Reducer Class1 : ```ErrorReducer```
    - sums up the time intervals with number of "ERROR" messages
- Mapper Class2 : ```Map2```
    - swaps position of key(time interval) and value(count) from previous reducer 
- Reducer Class1 : ```Reduce2```
    - reduces to key with count and value with time intervals of those counts 
    
###Task 3

- Mapper Class : ```MapperToken```
    - Creates key with type of log and value as 1
    
- Reducer Class : ```SumInt```
    - sums up values to give total number of specific type(ERROR, INFO, WARN, DEBUG) of logs

###Task 4

- Mapper Class : ``` CharMapper``` 
    - Maps keys to type of log and value to its length
- Reducer Class: ```MaxLen```
    - Finds max length of the types of logs

    
    