# BDP2
Big Data Project 2
## Instructions
1. Clone repo
2. Make sure you have the following python modules
    * 
3. ```python main.py```
## What happens now?
* Live tweets from twitter will start to be stored on a file localy. Once the time interval(which can be edited on the config.py file) it will send the tweets to HDFS. Once its on HDFS a script will analyze the data and store the results locally, which will update an html file with visualizations.
* While the tweets are being downloaded, a browser windows will pop up with a summary of some characteristics of twitter users(The repo comes with pre computed results so that the user doesn't have to wait. You can try to refresh the page every once in a while(depending on the interval time on the confgi.py) to see if the're new updates.
***INSERT PICS HERE*****
* While the program is running , data will be constanly being stored. You can cancel at any time.
