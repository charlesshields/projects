import requests
import json
import sqlalchemy
import logging
import time
import pytz
import datetime
import pandas as pd
import SQLLoggingHandler
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
from azure.storage.queue import QueueService

def init_logger():
    logger.setLevel(logging.INFO)

    # sqlite handler
    # sh = SQLLoggingHandler.SQLHandler(host="", port=1433, user="logicapp", passwd="", database="")
    # sh.setLevel(logging.INFO)
    # logger.addHandler(sh)

    # stdout handler
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    logger.addHandler(console)

def hr_merge():
    engine = create_engine("mssql+pyodbc://username:password@server/db?driver=''", pool_pre_ping = True)
    
    query = """
            SELECT *
            FROM [STAGING].[dbo].[LoadedHR]
            """

    df = pd.read_sql(query, engine)
    return df

def read_next_in_queue():
    try:
        messages = queue_service.get_messages(azureQueueRecognizedItems, num_messages=1)
        if messages:
            for message in messages:
                message_id = message.id
                final_frame(message.content)
                queue_service.delete_message(azureQueueRecognizedItems, message_id, message.pop_receipt)
        else:
            init_logger()
            logger.info("No Messages for TraxRecognizedItems In Queue")
    except Exception as e:
        init_logger()
        logger.error("Exception in read_next_in_queue {}" .format(e))


def final_frame(link):
    try:
        logger.info(f"recog_frame({link})")
        fmt = '%Y-%m-%d %H:%M:%S'
        slink = (requests.get(link).json())

        item_count = len(slink['details']['recognized_items'])
        current_session_uid = slink['session_uid']
        sinfo = pd.DataFrame(json_normalize(data = slink['details'], record_path=['recognized_items'], errors='ignore'))
        logger.info(f"session_uid: {current_session_uid} , Recognized Item Count: {item_count}")

        if item_count > 0:
            sinfo['SessionID'] = slink['session_uid']
            sinfo['SessionDate'] = slink['session_date']
            sinfo['SessionDateTime'] = slink['session_start_time']
            sinfo['EmailAddress'] = slink['visitor_identifier']
            sinfo['StoreNumber'] = slink['store_number']
            recog_items = pd.DataFrame(json_normalize(data = slink['details'], record_path = ['recognized_items', 'items'], meta=[['recognized_items', 'scene_uid']], errors='ignore'))
            

            recog_items.rename(columns={"recognized_items.scene_uid": "scene_uid"}, inplace=True)
            sinfo.drop(['items'], axis = 1, inplace = True)

            final_frame = pd.merge(recog_items, sinfo, how = 'left', sort=False)
            item_counts = pd.DataFrame(final_frame['count'].apply(pd.Series))

            final_frame = pd.concat([final_frame,item_counts], axis=1, sort=False)

            if "product_uuid" not in final_frame:
                final_frame['ProductID'] = pd.Series()
            else:
                final_frame.rename(columns={"product_uuid": "ProductID"}, inplace=True)

            final_frame.rename(columns={"scene_uid": "SceneID"}, inplace=True)
            final_frame.rename(columns={"task_code": "TaskCode"}, inplace=True)
            final_frame.rename(columns={"task_name": "TaskName"}, inplace=True)
            final_frame.rename(columns={"code": "ProductCode"}, inplace=True)
            final_frame.rename(columns={"name": "ProductName"}, inplace=True)
            final_frame.rename(columns={"type": "ProductType"}, inplace=True)
            final_frame.rename(columns={"total": "CountTotal"}, inplace=True)
            final_frame.rename(columns={"front": "CountFront"}, inplace=True)
            final_frame.rename(columns={"scene_uid": "SceneID"}, inplace=True)

            final_frame = final_frame[final_frame.StoreNumber.str.contains("50&")]
            final_frame['EmailAddress'] = final_frame['EmailAddress'].str.upper()
            final_frame['StoreNumber'] = final_frame['StoreNumber'].str[-9:]
            final_frame['UpdateDateTime'] = datetime.datetime.now(tz=pytz.utc).strftime(fmt)
            final_frame['JobName'] = 'Recognized Items'
            final_frame['SessionDateTime'] = pd.to_datetime(final_frame['SessionDateTime'], unit='s')
            final_frame['BODS_PROCESS_ID'] = ""
            final_frame['BODS_JOB_NAME'] = ""
            final_frame['BODS_UPDATE_TIMESTAMP'] = ""
            final_frame.fillna('', inplace=True)

            engine = create_engine("mssql+pyodbc://username:password@server/db?driver=''", pool_pre_ping = True)

            hr_data = hr_merge()
            hr_data.fillna('', inplace=True)

            recog_with_hr = pd.merge(final_frame, hr_data, how = 'left', on=('EmailAddress'), sort=False)
            recog_with_hr.rename(columns={"SNAME": "SName"}, inplace=True)

            recog_with_hr = recog_with_hr[['SessionID', 'SceneID', 'ProductID', 'StoreNumber', 'SessionDate', 'SessionDateTime', 'EmailAddress', 'TaskCode', 'TaskName', 'id',
            'ProductCode', 'ProductName', 'ProductType', 'CountTotal', 'CountFront', 'UpdateDateTime', 'JobName', 'SName', 'PositionText', 'BODS_PROCESS_ID', 'BODS_JOB_NAME', 'BODS_UPDATE_TIMESTAMP']]

            recog_with_hr.to_sql("TRAX_RECOGNIZED_ITEMS", engine, if_exists='append', chunksize=None, index=False)
        else:
            logger.warn(f"No Recognized Items in session_uid: {current_session_uid} , link: {link}")
    except ValueError:
        logger.error("Failed to decode JSON in {}".format(link))
    except KeyError:
        logger.error("No Recognized Items {}".format(link))
    except Exception:
        logger.error("Exception in final_frame")
        logger.warn(link)
        


if __name__ == '__main__':
    loggerName = "recognizeditems"
    logger = logging.getLogger(loggerName)

    azureQueueAccountName = ""
    azureQueueKey = ""
    azureQueueRecognizedItems = "recognizeditems-processing"
    queue_service = QueueService(account_name=azureQueueAccountName, account_key=azureQueueKey)
    try:

        while True:
            #get queue count
            metadata = queue_service.get_queue_metadata(azureQueueRecognizedItems)
            queue_count = metadata.approximate_message_count

            if queue_count > 0:
                read_next_in_queue()
            else:
                logger.info("time.sleep(3000)")
                time.sleep(3000)



    except Exception as e:
        init_logger()
        logger.error("Exception in main {}" .format(e))
