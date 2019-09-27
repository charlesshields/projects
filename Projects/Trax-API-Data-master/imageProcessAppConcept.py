#####################Directives######################
from flask import Flask,url_for
from applicationinsights.flask.ext import AppInsights
from azure.storage.queue import QueueService
from pandas.io.json import json_normalize
from sqlalchemy import create_engine

import pyodbc
import time
import pytz
import datetime
import os
import requests
import pandas as pd
##################End of Directives##################

# create Flask app
app = Flask(__name__)

# set the Application Insight Instrumentiation key to app.config
app.config['APPINSIGHTS_INSTRUMENTATIONKEY'] = ''

# initiate app to AppInsights
appinsights = AppInsights(app)

# force flushing application insights handler after each request
@app.after_request
def after_request(response):
    appinsights.flush()
    return response

@app.route('/')
@app.route('/home')
def home():
    return 'CCBCU Track Ingest Service'

#getanalysisresultspages will be invoked based on a regular cadence. 
#todo: can scale out by passing a Bottler ID as a parameter (parellalization)
@app.route("/getanalysisresultspages", methods=['GET', 'POST'])
def getanalysisresultspages():
    app.logger.debug('getanalysisresultspages function invoked')
    
    try: 
        #read the analysis result. 
        r = requests.get('')    
        messages = r.content 
        dicts = r.json()['results']
        for results in dicts:
            # print(results["results_link"])
            imageframe(results["results_link"])
 
    except Exception as e:
        app.logger("Exception in getanalysisresultspages {}".format(e))     
        
    app.logger.warn('exiting getanalysisresultspages')
    return "exiting getanalysisresultspages"

# Pass thru API to process results link for each session 
@app.route("/processresultlink/<link>", methods=['GET', 'POST'])
def processresultlink(slink):
    try:
        requests.get(url_for('http://127.0.0.1:5000/imageframe',link = slink))
        print(url_for('http://127.0.0.1:5000/imageframe',link = slink))
        app.logger.debug('processresultlink invoked for {}'.format(slink))
    except Exception as e:
        app.logger.error("Exception in processresultlink {}".format(e))     
        
    app.logger.debug('exiting processresultlink for {}'.format(slink))
    return 

# API To process the images 
@app.route("/imageframe/<link>", methods=['GET', 'POST'])
def imageframe(link):
    try:
        logger.debug("image_frame({})".format(link))
        fmt = '%Y-%m-%d %H:%M:%S'
        scene_df = pd.DataFrame()
        quality_df = pd.DataFrame()
        slink = (requests.get(link)).json() #response into readable data

        #check for image data in json
        image_count = len(slink['details']['images'])
        current_session_uid = slink['session_uid']
        logger.info("session_uid: {} , Image Count: {}".format(current_session_uid,image_count))
        print("session_uid: {} , Image Count: {}".format(current_session_uid,image_count))    
        if image_count > 0:
            scene_data = pd.DataFrame(json_normalize(data = slink['details'], record_path = ['images', 'scene_images'], 
            meta=[['images', 'scene_uid'],['images', 'task_code'],['images', 'task_name']], errors='ignore'))
            quality_issues = pd.DataFrame(json_normalize(data = slink['details'], record_path = ['images', 'scene_images', 'quality_issues'], meta = [['images', 'scene_images', 'image_uid']],  errors='ignore'))
            quality_df = pd.concat([quality_df, quality_issues], axis = 0, sort=True)

            scene_data['session_uid'] = slink['session_uid'] #adds top level info to dataframe
            scene_data['session_date'] = slink['session_date'] #adds top level info to dataframe
            scene_data['session_start_time'] = slink['session_start_time'] #adds top level info to dataframe
            scene_data['visitor_identifier'] = slink['visitor_identifier'] #adds top level info to dataframe
            scene_data['store_number'] = slink['store_number']
            scene_data['session_date'] = slink['session_date']

            scene_df = pd.concat([scene_df, scene_data], axis = 0, sort = True)		
            quality_df.rename(columns={"images.scene_images.image_uid": "image_uid"}, inplace=True)

            scene_df.rename(columns={"images.scene_uid": "scene_uid"}, inplace=True)
            scene_df.rename(columns={"images.task_code": "task_code"}, inplace=True)
            scene_df.rename(columns={"images.task_name": "task_name"}, inplace=True)

            image_urls = pd.DataFrame(scene_df['image_urls'].apply(pd.Series))

            scene_df.drop(['image_urls'], axis = 1, inplace=True)
            scene_df.drop(['quality_issues'], axis = 1, inplace=True)
            scene_df['ImageURL'] = image_urls['original'] #adds image from split

            image_frame = pd.merge(scene_df, quality_df, how='left', sort=False)

            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~DataFrame Remodeling~~~~~~~~~~~~~~~~~~~~~~~~~~
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

            # Renaming Columns to how the Business wants them
            image_frame.rename(columns={"scene_uid": "SceneID"}, inplace=True)
            image_frame.rename(columns={"task_code": "TaskCode"}, inplace=True)
            image_frame.rename(columns={"task_name": "TaskName"}, inplace=True)
            image_frame.rename(columns={"code": "QualityIssueCode"}, inplace=True)
            image_frame.rename(columns={"value": "QualityIssueValue"}, inplace=True)
            image_frame.rename(columns={"image_uid": "ImageID"}, inplace=True)
            image_frame.rename(columns={"session_uid": "SessionID"}, inplace=True)
            image_frame.rename(columns={"session_start_time": "SessionDateTime"}, inplace=True)
            image_frame.rename(columns={"session_date": "SessionDate"}, inplace=True)
            image_frame.rename(columns={"visitor_identifier": "EmailAddress"}, inplace=True)
            image_frame.rename(columns={"store_number": "StoreNumber"}, inplace=True)

            image_frame = image_frame[image_frame.StoreNumber.str.contains("50&")]
            image_frame['StoreNumber'] = image_frame['StoreNumber'].str[3:]

            image_frame['ImageCaptureTime'] = pd.to_datetime(image_frame['capture_time'], unit='s') #convert capture_time to UTC and change column name
            image_frame['SessionDateTime'] = pd.to_datetime(image_frame['SessionDateTime'], unit='s')
            image_frame.drop(['capture_time'], axis = 1, inplace=True) # drops the original capture time from dataframe
            image_frame['JobName'] = 'Analysis_Results_Images' # New column that contains the Job Name
            image_frame['UpdateDateTime'] = datetime.datetime.now(tz=pytz.utc).strftime(fmt) # New Column that contains the time the Job Ran

            #Reorganizing the columns of the DataFrame
            image_frame = image_frame[['SessionID', 'ImageID', 'QualityIssueCode', 'StoreNumber', 'SessionDate', 'SessionDateTime', 'EmailAddress', 'TaskCode', 'TaskName', 'ImageURL',
            'ImageCaptureTime', 'QualityIssueValue', 'UpdateDateTime', 'JobName']]

            #Get rid of the NULL values in the dataframe
            image_frame.fillna('', inplace=True)
            print('connecting to sql engine')
            engine = create_engine('mssql+pyodbc://username:password@server/db?driver=ODBC+Driver+##+for+SQL+Server')
            
            print('connected to sql engine')
            image_frame.to_sql("LoadedImages", engine, if_exists='append',chunksize=None, index=False)
            print('Inserted to SQL')
        else:
            logger.warn("No Images in session_uid: {} , link: {}".format(current_session_uid,link))

    except ValueError:
        logger.error("Failed to decode JSON in {}".format(link))
    except KeyError:
        logger.error("No Images in  {}".format(link))
    except Exception as e:
        logger.error("Exception in imageframe {}".format(e))  
        logger.warn(link)

if __name__ == "__main__":
    app.run(host="0.0.0.0")
