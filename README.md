<p align="center">Data Intensive Computing</br>Hadoop - MapReduce - Tableau</br>CSE 587
==========================================================================================
![Img_4](https://raw.githubusercontent.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/master/Images/4.png)

Goal
------
Learn Parallel Processing of Big Data using Hadoop MapReduce and Build Analysis and Visualization Dashboard(s) using Tableau

Context & Problem Statement
-------------------------------------------
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
	> PATH =  "$HADOOP_HOME/bin:${PATH}"<br />
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

Questions that we solved
------------------------------------
> **Question 1: UB Course Demand**</br>

> Every semester UB offers great courses to its students and provide the best world class faculty to teach those courses. However, we have observed that some of the courses are always in high demand either due to increasing technology demand of that field or due to extra ordinary teaching faculty. 

> UB student affairs want to globalize some of its courses which are highly demanding among students but we are not able to shortlist the courses. Can you utilize the “UB Course Scheduling data” to gather an insight on which courses have always been the first choice for the students?

>**Solution:**
> https://github.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/blob/master/Problem_2/Question_1_UB_Course_Demand/UBCourseDemand.java</br>
> Tableau Link: https://public.tableau.com/profile/publish/Q_1_UB_Course_Demand/P2_Q1#!/publish-confirm

---- 
> **Question 2: Most Popular Department Every Year**</br>
> We see that UB consists of many departments. However, which among these is the most popular? How do we decide which one is the most popular? Answering this question can give us an idea as to which department needs more classrooms and more time slots. 

> Since the popularity might change every year we also need to observe trends over the past few years. Therefore, our question would be, find the most popular department by number of enrollments every year.

>**Solution:**
> https://github.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/blob/master/Problem_2/Question_2_Popular_Department/PopularDepartment.java</br>
> Tableau Link: https://public.tableau.com/profile/publish/Q_2_Popular_Department/AnalysisofPopularDepartments#!/publish-confirm

---- 
> **Question 3: Find wasted space in every building over the years**</br>
> UB is broken into buildings and each building has a lab or a lecture hall. A lot of this space is usually wasted. 

> Find out which buildings have been historically wasting space and which buildings have been efficient. So that we can allot more classes in the inefficient buildings


>**Solution:**
> https://github.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/blob/master/Problem_2/Question_3_Building_Utilization/BuildingUtilization.java</br>
> Tableau Link: https://public.tableau.com/profile/publish/P2_Q3_0/P2_Q3#!/publish-confirm

---- 
> **Question 2: Most Popular Department Every Year**</br>
> We see that UB consists of many departments. However, which among these is the most popular? How do we decide which one is the most popular? Answering this question can give us an idea as to which department needs more classrooms and more time slots. 

> Since the popularity might change every year we also need to observe trends over the past few years. Therefore, our question would be, find the most popular department by number of enrollments every year.

>**Solution:**
> https://github.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/blob/master/Problem_2/Question_2_Popular_Department/PopularDepartment.java</br>
> Tableau Link: https://public.tableau.com/profile/publish/Q_2_Popular_Department/AnalysisofPopularDepartments#!/publish-confirm

---- 
> **Question 4: Lecture Time Analysis**</br>
> Class scheduling is very complex problem and its all the more difficult in a department where the enrollments are increasing. We have observed that some of the classes are small as compared to the number of students enrolled and vice versa. This not only make it difficult for the professor to deliver his/her lecture efficiently but also creates a bad image of us among international students.

> Being a World Class University we do not want our students to fight for a seat during the class. Can you utilize the “UB Course Scheduling data” to provide an insight on what time of the day have remained most occupied with lectures during the years and what is the most suitable time to reschedule these courses, so that we can allocate them class of proper size?

>**Solution:**
> https://github.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/blob/master/Problem_2/Question_4_LectureTime_Analysis/LectureTimeAnalysis.java</br>
> Tableau Link: https://public.tableau.com/profile/publish/Q_4_LectureTime_Analysis/P2_Q4#!/publish-confirm

---- 
> **Question 5: Find the most popular exam slots in every building**</br>
> Exam season is stressful for everyone. For the students and especially for the staff. They have to ensure classes are allotted and everyone finds their place. From the 6 years’ exam data that we have we aim to find the most popular exam slots in every building. By this data we can find out what time slot most exams are held. We can further analyze this data and make decisions to move some of the exams at this time slot to another. We can also make decisions to move them to other halls to.

>**Solution:**
> https://github.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/blob/master/Problem_2/Question_5_Popular_Exam_Slots/PopularExamSlots.java</br>
> Tableau Link: https://public.tableau.com/profile/publish/Q_5_Popular_Exam_Slots/P2_Q5#!/publish-confirm


Credits
----------
 We acknowledge and grateful to Professor Bina Ramamurthy (http://www.cse.buffalo.edu/faculty/bina/) and TA Junfei Wang (https://www.linkedin.com/in/junfei-wang-5971a848) for their continuous support throughout the course that helped us learn the skills of Data Intensive Computing
 
 
Contributors
------------------
 ![Img_5](https://raw.githubusercontent.com/ramanpreet1990/CSE_587_Data_Intensive_Computing/master/Images/5.png)

Ramanpreet Singh Khinda (rkhinda@buffalo.edu)</br>
https://branded.me/ramanpreet1990

Elroy Preetham Alva (elroypre@buffalo.edu)
 

License
-------
Copyright {2016} 
{Ramanpreet Singh Khinda rkhinda@buffalo.edu & Elroy Preetham Alva elroypre@buffalo.edu} 

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


Important
---------
You are authorized to use the source code and make modifications as applicable by the **above Licence** however you are **not authorized** to make use of the university protected classroom scheduling data that is used in this project
