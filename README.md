# <p align="center">Data Intensive Computing<br/>Hadoop Project</p>

<p align="center">Goal: Learn Parallel Processing of Big Data using Hadoop MapReduce and a Build a Dashboard(s) for Analysis and Visualization of the Results</p>

<p align="center">Due date: 4/16/2016 by 11.59PM or earlier, online submission on cse machines</p>

## Important Links
1. https://getblueshift.com/setting-up-hadoop-2-4-and-pig-0-12-on-osx-locally/
2. http://www.mkyong.com/java/how-to-set-java_home-environment-variable-on-mac-os-x/


## Path Setup for Mac
$ nano .bash_profile

----------------------------------------------------------------------------------------
# Java
export JAVA_HOME=$(/usr/libexec/java_home -v 1.7)

# Hadoop
export HADOOP_HOME=/Users/raman/Documents/SUNY_BUFFALO/DIC_CSE_587/hadoop-2.6.4

PATH="$HADOOP_HOME/bin:${PATH}"

export PATH
----------------------------------------------------------------------------------------

Save file by executing
$ source .bash_profile


************************************************************************************************************

## Initial Steps to Launch Hadoop
1. Download Hadoop as per the Guidelines attached
2. Update the required files

$ cd /usr/local/Cellar/hadoop/2.4.1
$ ./bin/hdfs namenode -format
$ ./sbin/start-dfs.sh
$ ./bin/hdfs dfs -mkdir /user
$ ./bin/hdfs dfs -mkdir /user/<username>
$ ./sbin/start-yarn.sh
$ jps


## Ensure below criteria is met
1. Ensure 5 processes are running
2. http://localhost:50070
3. http://localhost:8088


## Start Hadoop
$ ./sbin/start-dfs.sh
$ ./sbin/start-yarn.sh


## Stop Hadoop
$ ./sbin/stop-dfs.sh
$ ./sbin/stop-yarn.sh


************************************************************************************************************

## Settings for Hadoop Setup on Eclipse
1. Download the Jar and Add in the Plugin Folder of Eclipse
https://github.com/winghc/hadoop2x-eclipse-plugin/blob/master/release/hadoop-eclipse-kepler-plugin-2.4.1.jar

2. Check run configurations on Eclipse
Arguments
hdfs://localhost:9000/user/<username>/input/ hdfs://localhost:9000/user/<username>/output/

3. Set Map Reduce Location
Left side: 50070 and Right side: 9000

4. After running the source code make sure to delete the output file

************************************************************************************************************


## Basic HDFS Commands
1. hdfs dfs -ls /
2. hdfs dfs -mkdir /user/<username>/input
3. hdfs dfs -put <source file path> /user/<username>/input
4. hdfs dfs -cat /user/<username>/output/<output file>
5. hdfs dfs -get output <local_folder>


#####<p align="center">Contributors</p>
<p align="center">Ramanpreet Singh Khinda</p>
<p align="center">Elroy Alva</p>
