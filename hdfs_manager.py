import config
from hdfs import InsecureClient
from time import strftime, gmtime
import subprocess
import time

current_milli_time = lambda: int(round(time.time() * 1000))


class HDFSManager:

    def __init__(self):

        # Start communication with HDFS
        self.client_hdfs = InsecureClient(config.HDFS_SERVER) #toDO STILL NECESARY?

        # Create folder path
        subprocess.Popen(['hdfs -dfs mkdir ' + config.OUTPUT_FILE_PATH], shell=True)

        # Start tracking time
        self.tick = current_milli_time

        # Define file path
        self.currentfile = config.OUTPUT_FILE_PATH + strftime("%d%b%Y_%H%M%S", gmtime()) + ".json"

    def update_file_list(self):

        # Creates list to keep track of files
        filelist = open(config.LIST_PATH + "tweetsList.txt", "a+")
        filelist.write(self.currentfile + "\n")
        filelist.close()

        # Define new file path
        self.currentfile = config.OUTPUT_FILE_PATH + strftime("%d%b%Y_%H%M%S", gmtime()) + ".json"

        # Restart countdown
        self.tick = current_milli_time

    def save_tweet(self, tweet):

        try:
            output = open("./temp.json", "a+", encoding='utf-8')
            output.write(str(tweet) + '\n')
        except Exception as e:
            print(e)
        finally:
            output.close()

        #Once the interval is done, send file to HDFS and start a new one.
        if self.tick - current_milli_time > config.INTERVAL * 1000:

            # Send new file to HDFS
            put = subprocess.Popen(['hdfs fs -put ./temp.json ' + config.OUTPUT_FILE_PATH], shell=True)
            put.communicate()

            # Rename in HDFS
            put = subprocess.Popen(['hadoop fs -mv ' + config.OUTPUT_FILE_PATH + '/temp.json ' + self.currentfile])

            self.update_file_list()

            # Clear temp file
            open("./temp.json", "w").close()

