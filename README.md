<p align="center">Data Intensive Computing</p>
<p align="center">Hadoop - MapReduce - Tableau</p>
<p align="center">CSE 587</p>
==================================================


**Goal**</ br>
Learn Parallel Processing of Big Data using Hadoop MapReduce and Build Analysis and Visualization Dashboard(s) using Tableau

**Context of Problem Statement**</ br>
Class room scheduling for courses is complex problem. It is all the more difficult in a department where the enrollments are increasing and number of courses and class sizes are increasing. You are given the data for courses and class rooms from 1931 to 2017. Data analysis you perform will be driven by problems you want to solve and questions you want answered. Users for this “data product” are (i) course schedulers (ii) teachers teaching a course (iii) university planners and (iv) university development department for fund raising related activities.

Understand the domain and design a list of relevant questions. You are required to design and implement MR algorithms to extract useful intelligence to answer the questions. This intelligence will be offered as answers to questions through a web interface (dashboard). The answers to these questions could be through visualization, textual output or numerical information. The dashboard will feature ability for the user to interact by choosing from a set of questions and also by configuring the parameters for the questions and the visualizations. Sample questions to get you started are given below one for each type of user: (i) List all the class rooms of capacity greater than 200 for Mondays and Wednesdays between 5 and 6.20PM for Spring 2016 (ii) List a class room with capacity greater than 200 for May 9, 2016, 7-11pm (iii) track the trend in growth of seats per semester per building in the last 10 years in an interactive visualizations and (iv) compare enrollment increase over last 10 years to the building space increase in a visualization.

More specifically we want to analyze the data and provide a presentation of the charts (in the form of the dashboards) explaining the usage of the spaces (classrooms). How it has changed, what is trending, who is using the classrooms. Are the rooms being efficiently used?  Why was  CSE 4/587 scheduled in a 150 capacity NSC 215 when there was a classroom with modern facilities available at Knox 109 and Knox 110?

Steps for Hadoop Setup
---------------------------------
You can either run Hadoop MR jobs in HDFS which require you to run the HDFS every time or you can also do it locally by **adding Hadoop jars in Eclipse**
 
1. Follow Tutorials from the mentioned links (make sure to set the **JAVA_HOME** and **HADOOP_HOME**  path in your **bash_profile**)
	> - https://getblueshift.com/setting-up-hadoop-2-4-and-pig-0-12-on-osx-locally/
	> -  http://www.mkyong.com/java/how-to-set-java_home-environment-variable-on-mac-os-x/
	> -  http://www.tutorialspoint.com/hadoop/hadoop_enviornment_setup.htm

2. Edit **bash_profile**<br />
	$ nano .bash_profile<br />
	> export JAVA_HOME = $(/usr/libexec/java_home -v 1.7)<br />
	> export HADOOP_HOME = */localpath/hadoop-2.6.4*<br />
	> PATH =  "$\$$HADOOP_HOME/bin:$\$${PATH}"<br />
	> export PATH<br />

	$ source .bash_profile
 

3. Download the Jar from the below link and copy paste it in the **Plugin Folder** of Eclipse
	https://github.com/winghc/hadoop2x-eclipse-plugin/blob/master/release/hadoop-eclipse-kepler-plugin-2.4.1.jar

4. Goto Project Build path and add Hadoop jars as shown below
![Img_1](https://raw.githubusercontent.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/master/Images/1.png)

5. Configure program parameters in run configuration
![Img_2](https://raw.githubusercontent.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/master/Images/2.png)

	*Alternatively if you are running Hadoop on HDFS than you may want to give the HDFS path for the Arguments as shown below
> hdfs://localhost:9000/user/rkhinda/input/ hdfs://localhost:9000/user/rkhinda/output/

6.  Define the location of a Hadoop infrastructure for running MapReduce applications
![Img_3](https://raw.githubusercontent.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/master/Images/3.png)
