import tempfile
import datetime
import json
import logging
import xlsxwriter
import pyodbc
import pysftp
import time
import requests
import configparser
import math
import pandas as pd
import sqlalchemy
import base64
import numpy as np
import os, uuid, sys
from sqlalchemy import create_engine
from pandas.io.json import json_normalize
from configparser import ConfigParser
from io import StringIO
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.queue import QueueService


#################################################################################################################################################
#################################################################################################################################################

# Start Up Logger
logging.basicConfig(level=logging.ERROR,
                    format='%(message)s',
                    datefmt='%m-%d %H:%M')
logger = logging.getLogger("root")
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
logger.addHandler(ch)




def GetValue(configSection, configpath):
    return parser.get(configSection, configpath)


# ################################################################################################################################################
# ################################################################################################################################################


def read_next_in_queue():
    rson = []
    try:
        queue_service
        messages = queue_service.get_messages(azureQueueParameters, num_messages = 1)
        if messages:
            for message in messages:
                message_id = message.id
                r = message.content
                r = base64.b64decode(r).decode('utf-8')
                rson = r.split()
                queue_service.delete_message(azureQueueParameters, message_id, message.pop_receipt)
        else:
            print("Queue Count = 0")
    except Exception as e:
        print("Exception in read_next_in_queue {}" .format(e))
    else:
        return rson

# ################################################################################################################################################
# ################################################################################################################################################

# Def Blob Service
# Generate Blob regardless of name from blob storage.
# convert data types to str and remove zeros from left if they are there
def blob_read(ord_loc, file_name):
    try:
        block_blob_service = BlockBlobService(
            account_name= GetValue('PARAMETERS_BLOB', 'account_name'),
            account_key=GetValue('PARAMETERS_BLOB', 'account_key')
            )
        generator = block_blob_service.list_blobs(ord_loc)
        for blob in generator:
            if blob.name == file_name:
                blobstring = block_blob_service.get_blob_to_text(ord_loc, blob.name).content
                infile = pd.read_csv(StringIO(blobstring))
                nanfile = infile[infile['Customer'].isnull() | infile['MaterialNumber Or PackUPC'].isnull() | infile['SuggestedCaseVolume'].isnull()]
                if nanfile.size > 0:
                    raise Exception("There should not be ")
                infile['Customer'] = infile['Customer'].astype(str)
                infile['Customer'] = infile['Customer'].str.lstrip("0")
                if 'MaterialNumber Or PackUPC' in infile.columns:
                    infile['MaterialNumber Or PackUPC'] = infile['MaterialNumber Or PackUPC'].astype(str)
                    infile['MaterialNumber Or PackUPC'] = infile['MaterialNumber Or PackUPC'].str.lstrip("0")
                    if (len(infile['MaterialNumber Or PackUPC'][1]) == 6) == True:
                        infile = infile.rename(columns={'MaterialNumber Or PackUPC': "MaterialNumber"})
                    elif (len(infile['MaterialNumber Or PackUPC'][1]) == 6) == False:
                        infile = infile.rename(columns={'MaterialNumber Or PackUPC': "PackUPC"})

                    else:
                        print("FAILURE on columns in blob_read")
                else:
                    print("First If Inside For Failure")
                return infile
    except Exception as e:
        print("Exception in blob_read {}" .format(e))
        
# ################################################################################################################################################
# ################################################################################################################################################


# Def Blob Service
# Generate Blob regardless of name from blob storage.
# convert data types to str and remove zeros from left if they are there
def factory_reset(factory):
    try:
        block_blob_service = BlockBlobService(
            account_name= GetValue('PARAMETERS_BLOB', 'account_name'),
            account_key=GetValue('PARAMETERS_BLOB', 'account_key')
            )
        generator = block_blob_service.exists(factory, 'reset.csv')
        while generator == True:
            if generator == True:
                generator = block_blob_service.exists(factory, 'reset.csv')
                time.sleep(3)
            if generator == False:
                break
        
    except Exception as e:
        print(e)

# ################################################################################################################################################
# ################################################################################################################################################
        
def toblob(file_path, container_name):
    try:
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(
            account_name= GetValue('PARAMETERS_BLOB', 'account_name'),
            account_key=GetValue('PARAMETERS_BLOB', 'account_key')
            )
        # Create a container called 'pythoncontainer'.
        container_name = container_name
        block_blob_service.create_container(container_name, fail_on_exist = False)
        # Set the permission so the blobs are public.
        block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
        # Gets a file from path described to test the upload and download.
        os.path.expanduser('~/temp/')
        dirname, basename = os.path.split(file_path)
        local_file_name = basename
        full_path_to_file = os.path.join(dirname, basename)
        #print("Uploading to Blob storage as blob" + local_file_name)
        # Upload the created file, use local_file_name for the blob name
        block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)
        # List the blobs in the container
        # print(\\\nList blobs in the container\)
        # generator = block_blob_service.list_blobs(container_name)
        # for blob in generator:
        #    print(\\\t Blob name: \ + blob.name)
        # Download the blob(s).
        # Add '_DOWNLOADED' as prefix to '.txt' so it makes a record of what was downloaded to blob storage.
        # full_path_to_file2 = os.path.join(local_path, str.replace(local_file_name ,'.txt', '_DOWNLOADED.txt'))
        # print(\\\nDownloading blob from \ + full_path_to_file2)
        # block_blob_service.get_blob_to_path(container_name, local_file_name, full_path_to_file2)
        #sys.stdout.write("Loaded file to blob. Application will end when you hit <any key>")
        sys.stdout.flush()
        #input()
        # Clean up resources. This includes the container and the temp files
        # Only uncomment if you are testing
        #block_blob_service.delete_container(container_name)
        # os.remove(full_path_to_file)
        # os.remove(full_path_to_file2)
    except Exception as e:
        print("Exception in toblob {}" .format(e))

# ################################################################################################################################################
# ################################################################################################################################################

def blob_delete(ord_loc, file_name):
    try:
        block_blob_service = BlockBlobService(
            account_name= GetValue('PARAMETERS_BLOB', 'account_name'),
            account_key=GetValue('PARAMETERS_BLOB', 'account_key')
            )
        generator = block_blob_service.list_blobs(ord_loc)
        for blob in generator:
            if blob.name == file_name:
                block_blob_service.delete_blob(ord_loc, file_name)
    
    except Exception as e:
        print("Exception in blob_delete {}" .format(e))

# ################################################################################################################################################
# ################################################################################################################################################


#################################################################################################################################################
#################################################################################################################################################
#################################################################################################################################################

# This function is in case they pass a six digit material number instead of the UPC Code
# This will query SAP HANA and get the UPC code for the material at the plant of the customer.
def get_UPC(indblock_pass):
    try:
        engine = create_engine(GetValue('SQLCONNECTION', 'connection_string'), pool_pre_ping=True)
        sqlquery = """
                SELECT Plant, MaterialNumber, PackUPC
                FROM [STAGING].[dbo].[SOU_Material]
                """
                # WHERE "MaterialType" = 'ZFER'
        df = pd.read_sql(sqlquery, engine)
        df = df.fillna('')
        df['MaterialNumber'] = df['MaterialNumber'].str.lstrip("0")
        df['PackUPC'] = df['PackUPC'].str.lstrip("0")
        

        cust_pass = pd.merge(indblock_pass, df, how = 'left', left_on = ['SalesOffice', 'MaterialNumber'], right_on = ['Plant', 'MaterialNumber'])
        cust_pass = cust_pass.drop("Plant", axis = 1)
        
        #connection.close()
    except Exception as e:
        print("Exception in get_UPC {}" .format(e))
    else:
        return cust_pass

#################################################################################################################################################
#################################################################################################################################################
#################################################################################################################################################

# This function gather customer data from SAP HANA and merges it onto the given customer list ingested from the blob
def customer_data(infile):
    try:
        engine = create_engine(GetValue('SQLCONNECTION', 'connection_string'), pool_pre_ping=True)

        sqlquery = """
        SELECT SalesOffice, CustomerNumber, PODSDIndicator, CentralOrderBlock, Name1, AuthMaterialListID, Tradename, DeliveringPlant
        FROM [STAGING].[dbo].[SOU_Customer]
        """
        # WHERE "SalesOffice" != '' AND "Tradename" NOT IN('', '99999')

        cust_data = pd.read_sql(sqlquery, engine)
        cust_data = cust_data.fillna('')
        cust_data['CustomerNumber'] = cust_data['CustomerNumber'].str.lstrip("0")

        cust_result = pd.merge(infile, cust_data, how = 'left', left_on = 'Customer', right_on = 'CustomerNumber')
        cust_result.fillna('', inplace = True)
        no_exist = cust_result[(cust_result.CustomerNumber.str.len() == 0) == True]
        cust_result_pass = cust_result[(cust_result.CustomerNumber.str.len() == 0) == False]
        cust_result_pass = cust_result_pass.drop_duplicates(['CustomerNumber', 'MaterialNumber'], keep = 'first')
        cust_result_pass = cust_result_pass.drop(['Customer'], axis = 1)

    except Exception as e:
        print("Exception in customer_data {}" .format(e))
    else:
        return cust_result_pass, no_exist

# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################

#This function calls a valid determination table from SAP HANA and puts it into a dataframe for merging purposes later in the process.
def determinate():
    #"WERKS", KUNNR", "MATWA", "SMATN", "_SCL_TRADENAME", "WERKS", "MATWA", "SMATN"
    try:
        engine = create_engine(GetValue('SQLCONNECTION', 'connection_string'), pool_pre_ping=True)

        sqlquery = """
        SELECT DeterminationType, Source, WERKS, KUNNR, MATWA, SMATN, _SCL_TRADENAME
                FROM [STAGING].[dbo].[SOU_MaterialDetermination]
        """
        # WHERE DeterminationType = 'Z001' AND MaterialDeterminationStillValidFlag = 'Y'
        df = pd.read_sql(sqlquery, engine)
        df = df.fillna('')
        #connection.close()
    except Exception as e:
        print("Exception in determinate {}" .format(e))
    else:
        return df

# ################################################################################################################################################
# ################################################################################################################################################

# This function makes a small dataframe from SAP HANA Material Plants data for reference to check and see if materials are blocked on the plant level.
# This function can be taken out and absorbed into the other function
def block_status():
    try:
        engine = create_engine(GetValue('SQLCONNECTION', 'connection_string'), pool_pre_ping=True)
        sqlquery = """
                SELECT Plant, MaterialNumber, MaterialStatus, ValuatedStock
                FROM [STAGING].[dbo].[SOU_Material]
                """
                #Material Type = ZFER
        
        df = pd.read_sql(sqlquery, engine)
        df = df.fillna('')
        df['MaterialNumber'] = df['MaterialNumber'].str.lstrip("0")
        #connection.close()
    except Exception as e:
        print("Exception in block_status {}" .format(e))
    else:
        return df

# ################################################################################################################################################
# ################################################################################################################################################

# This function created a reference dataframe for the Customer_AMLExclusionType view. This will check AML's that are type Z002, meaning Customer AMLs.
def exclusionZ002():
    #"WERKS", KUNNR", "MATWA", "SMATN", "_SCL_TRADENAME", "WERKS", "MATWA", "SMATN"
    try:
        engine = create_engine(GetValue('SQLCONNECTION', 'connection_string'), pool_pre_ping=True)
        sqlquery = """
                SELECT *
                FROM [STAGING].[dbo].[SOU_AmlExclusionType]
                WHERE KSCHL = 'Z002' AND VTWEG = 'Z1'
                """

        df = pd.read_sql(sqlquery, engine)
        df = df.fillna('')
        #connection.close()
    except Exception as e:
        print("Exception in exclusionZ002 {}" .format(e))
    else:
        return df

# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################

# This function created a reference dataframe for the Customer_AMLExclusionType view. This will check AML's that are type Z000, meaning Sales Office AMLs.
def exclusionZ000():
    #"WERKS", KUNNR", "MATWA", "SMATN", "_SCL_TRADENAME", "MATWA", "SMATN"
    try:
        engine = create_engine(GetValue('SQLCONNECTION', 'connection_string'), pool_pre_ping=True)
        sqlquery = """
                SELECT *
                FROM [STAGING].[dbo].[SOU_AmlExclusionType]
                WHERE KSCHL = 'Z000' and VTWEG = 'Z1'
                """

        df = pd.read_sql(sqlquery, engine)
        df = df.fillna('')
        #connection.close()
    except Exception as e:
        print("Exception in exclusionZ000 {}" .format(e))
    else:
        return df
# ################################################################################################################################################
# ################################################################################################################################################
# ################################################# DETERMINATION CHECKER ########################################################################
# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################

# The following 5 functions are chained together to check different determinations at different points in the chain, and generate the failed data properly if there is any.
# determination is checked in this order: 910, 914, 917, 913, SOrg

def nineten(cust_pass, determinate):
    try:
        determinate = determinate[(determinate.Source.str.contains("910") == True)]
        result = pd.merge(cust_pass, determinate, how='left', left_on = ('CustomerNumber', 'PackUPC'), right_on = ('KUNNR', 'MATWA'))
        result = result.fillna('')
        passed_910 = result[(result.WERKS.str.len() == 0) == False]
        failed_910 = result[((result.WERKS.str.len() == 0) | (result.PackUPC.str.len() == 0)) == True]
    
    except Exception as e:
        print("Exception in nineten {}" .format(e))
    else:
        return passed_910, failed_910

# ################################################################################################################################################
# ################################################################################################################################################

def ninefourteen(failed_910, determinate):
    try:
        failed_910 = failed_910.drop(['DeterminationType', 'Source', 'WERKS', 'KUNNR', 'MATWA', 'SMATN', '_SCL_TRADENAME'], axis = 1)
        determinate = determinate[(determinate.Source.str.contains("914") == True)] 
        result = pd.merge(failed_910, determinate, how = 'left', left_on = ('PackUPC', 'DeliveringPlant', 'Tradename'), right_on = ('MATWA', 'WERKS', '_SCL_TRADENAME'))
        result = result.fillna('')
        passed_914 = result[(result.WERKS.str.len() == 0) == False]
        failed_914 = result[(result.WERKS.str.len() == 0) == True]
    
        
    except Exception as e:
        print("Exception in ninefourteen {}" .format(e))
    else:
        return passed_914, failed_914

# ################################################################################################################################################
# ################################################################################################################################################

def nineseventeen(failed_914, determinate):
    try:
        failed_914 = failed_914.drop(['DeterminationType', 'Source', 'WERKS', 'KUNNR', 'MATWA', 'SMATN', '_SCL_TRADENAME'], axis = 1)
        determinate = determinate[(determinate.Source.str.contains("917") == True)] 
        result = pd.merge(failed_914, determinate, how = 'left', left_on = ('PackUPC', 'Tradename'), right_on = ('MATWA', '_SCL_TRADENAME'))
        result = result.fillna('')
        passed_917 = result[(result.WERKS.str.len() == 0) == False]
        failed_917 = result[(result.WERKS.str.len() == 0) == True]

        
    except Exception as e:
        print("Exception in nineseventeen {}" .format(e))
    else:
        return passed_917, failed_917

# ################################################################################################################################################
# ################################################################################################################################################

def ninethirteen(failed_917, determinate):
    try:
        failed_917 = failed_917.drop(['DeterminationType', 'Source', 'WERKS', 'KUNNR', 'MATWA', 'SMATN', '_SCL_TRADENAME'], axis = 1)
        determinate = determinate[(determinate.Source.str.contains("913") == True)] 
        result = pd.merge(failed_917, determinate, how = 'left', left_on = ('PackUPC', 'DeliveringPlant'), right_on = ('MATWA', 'WERKS'))
        result.fillna('', inplace=True)
        passed_913 = result[(result.WERKS.str.len() == 0) == False]
        failed_913 = result[(result.WERKS.str.len() == 0) == True]
        
        
    except Exception as e:
        print("Exception in ninethirteen {}" .format(e))
    else:
        return passed_913, failed_913

# ################################################################################################################################################
# ################################################################################################################################################

def sales_org(failed_913, determinate):
    try:
        failed_913 = failed_913.drop(['DeterminationType', 'Source', 'WERKS', 'KUNNR', 'MATWA', 'SMATN', '_SCL_TRADENAME'], axis = 1)
        determinate = determinate[(determinate.Source.str.contains("SOrg") == True)]
        result = pd.merge(failed_913, determinate, how = 'left', left_on = ('PackUPC'), right_on = ('MATWA'))
        result = result.fillna('')
        passed_sorg = result[(result.WERKS.str.len() == 0) == False]
        failed_sorg = result[(result.WERKS.str.len() == 0) == True]
    
        
    except Exception as e:
        print("Exception in sales_org {}" .format(e))
    else:
        return passed_sorg, failed_sorg

# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################


# ################################################################################################################################################
# ################################################################################################################################################

# This function uses the blocking data DataFrame from the earlier view generated to merge with the data that has passed so far.
def block_check(matblocked, df_all):
    try:
        matblock_check = pd.merge(df_all, matblocked, how = 'left', left_on = ('SalesOffice', 'SMATN'), right_on = ('Plant', 'MaterialNumber'))
        #matblock_check = matblock_check.drop(['Plant', 'MaterialNumber'], axis = 1)
        matblock_check = matblock_check.fillna('')
        matblock_pass = matblock_check[(matblock_check.MaterialStatus.str.len() == 0) == True]
        matblock_copy = matblock_pass.copy()
        matblock_fail = matblock_check[(matblock_check.MaterialStatus.str.len() > 0) == True]

    except Exception as e:
        print("Exception in block_check {}" .format(e))
    else:
        return matblock_pass, matblock_fail, matblock_copy
        
# ################################################################################################################################################
# ################################################################################################################################################

def inventory(matblock_copy, matblocked):
    try:
        inv = matblock_copy.groupby(['DeliveringPlant', 'SMATN']).count()[['SuggestedCaseVolume']].reset_index()
        # Descending Percentage added to the calculated column

        df = pd.merge(inv, matblocked, how = 'left', left_on = ('DeliveringPlant', 'SMATN'), right_on = ('Plant', 'MaterialNumber'))
        df['Consumed Percentage'] = df.SuggestedCaseVolume/df.ValuatedStock.replace({ 0 : np.nan })
        df.drop(['MaterialStatus'], axis = 1, inplace = True)
        df.fillna(1, inplace = True)
        df = df.sort_values(by=['Consumed Percentage'], ascending = False)
        
    except Exception as e:
        print("Exception in inventory {}" .format(e))
    else:
        return df

# ################################################################################################################################################
# ################################################################################################################################################

# This function merges the data that passed determination with Z000 AML Exclusion Data to check and see if it is on the Sales Office AML.
def soaml(Z000, passed_det):
    try:
        dfZ000 = pd.merge(passed_det, Z000, how='left', left_on = ('SalesOffice', 'SMATN'), right_on = ('_SCL_AUTH_MATLST', 'MATNR'))
        dfZ000 = dfZ000.fillna('')
        dfZ000_fail = dfZ000[(dfZ000.MANDT.str.len() == 0) == True]
        dfZ000 = dfZ000[(dfZ000.MANDT.str.len() == 0) == False]
        
    except Exception as e:
        print("Exception in soaml {}" .format(e))
    else:
        return dfZ000, dfZ000_fail

# ################################################################################################################################################
# ################################################################################################################################################

# This function merges the data that passed determination with Z002 AML Exclusion Data to check and see if it is on the Customer AML.
def cusaml(Z002, dfZ000):
    try:
        dfZ000_copy = dfZ000.copy()
        dfZ000_copy = dfZ000_copy[(dfZ000_copy.AuthMaterialListID.str.contains("A0")) == False]
        dfZ000_copy = dfZ000_copy.drop(['MANDT', 'KAPPL', 'KSCHL', 'VKORG', 'VTWEG', '_SCL_AUTH_MATLST', 'MATNR', 'DATBI', 'DATAB'], axis = 1)
        dfZ002 = pd.merge(dfZ000_copy, Z002, how='left', left_on = ('AuthMaterialListID', 'SMATN'), right_on = ('_SCL_AUTH_MATLST', 'MATNR'))
        dfZ002 = dfZ002.fillna('')
        dfZ002_copy = dfZ002.copy()
        dfZ002_fail = dfZ002_copy[(dfZ002_copy.MANDT.str.len() == 0) == True]
        dfZ002 = dfZ002[(dfZ002.MANDT.str.len() == 0) == False]
        
    except Exception as e:
        print("Exception in cusaml {}" .format(e))
    else:
        return dfZ002, dfZ002_fail

# ################################################################################################################################################
# ################################################################################################################################################

# This function calls the CONA CUSTOMER API if there are no dates given with the original blob data.
# The API will get the next delivery day based on the date passed with the blob input from the power app.
def cona_api(ord_loc, file_name, ord_date, matblock_pass):
    user = GetValue('CONA_API', 'username')
    password = GetValue('CONA_API', 'password')
    headers={'Content-type':'application/json', 'Accept':'application/json'}
    url = GetValue('CONA_API', 'cona_url')
    try:
        total_del_dates = pd.DataFrame()
        dfc = matblock_pass['CustomerNumber'].unique().tolist()
        split_factor = math.ceil(len(dfc)/200)
        for i in range(0,split_factor):
            body = {"salesOrg": "4100","distributionChannel": "","startDate": "{}".format(ord_date),"division": "","numberOfDates": "1","salesDocumentType": "ZOR","shippingCondition": "","visitPlanType": "Z7","customers":[]}
            if len(dfc) < 200:
                dfcdiv = dfc[:]
                dfc = []
            else:
                dfcdiv = dfc[0:200]
                dfc = dfc[200:]
            for cust in dfcdiv:
                body['customers'].append({"number":"0"+"{}".format(cust)})
            r = requests.post(url, data = json.dumps(body), auth=(user, password), headers = headers).json()
            del_dates = pd.DataFrame(json_normalize(data = r['deliveryDates'], record_path = ['date'], errors='ignore'))
            total_del_dates = total_del_dates.append(del_dates)
        
        total_del_dates['customerNumber'] = total_del_dates['customerNumber'].str.lstrip("0")
        total_del_dates['deliveryDate'] = pd.to_datetime(total_del_dates['deliveryDate']).dt.strftime('%m/%d/%Y')
        total_del_dates['customerNumber'] = total_del_dates['customerNumber'].astype(str)
        # print(total_del_dates)

        

        added_dates = pd.merge(matblock_pass, total_del_dates, how='left', left_on= 'CustomerNumber', right_on= 'customerNumber')
        added_dates['customerNumber'] = added_dates['customerNumber'].str.zfill(10)

    except Exception as e:
        print("error in cona_api {}".format(e))

    else:
        return added_dates


# ################################################################################################################################################
# ################################################################################################################################################

#This function cleans up the final dataframe with the condition that it didn't have dates to begin with and adds the dates in via the CONA API.
# The function has so if statements in place depending on parameters passed with the blob input via the power app. Namely order type: (Draft or ForceShipment)
# make some if statements with the ord_type
def clean_added_dates(added_dates, ord_type):
    try:
        #added_dates = added_dates.drop(['Source', 'CustomerNumber', 'PackUPC', 'DATBI', 'DATAB', 'MaterialStatus', '_SCL_AUTH_MATLST', 'KSCHL', 'KAPPL', 'MANDT', '_SCL_TRADENAME', 'SMATN', 'MATWA', 'KUNNR', 'WERKS', 'DeterminationType', 'DeliveringPlant', 'Tradename', 'AuthMaterialListID', 'Name1', 'CentralOrderBlock', 'PODSDIndicator', 'SalesOffice'], axis = 1)
        added_dates = added_dates.rename(columns={"VKORG": "Sales Org"})
        added_dates = added_dates.rename(columns={"VTWEG": "DC"})
        added_dates = added_dates.rename(columns={"SuggestedCaseVolume": "Order Quantity"})
        added_dates = added_dates.rename(columns={"SMATN": "Material Number"})
    
        if 'deliveryDate' in added_dates:
            added_dates = added_dates.rename(columns={"deliveryDate": "Delivery Date"})
    
        added_dates = added_dates.rename(columns={"CustomerNumber": "Sold to"})
        added_dates['Sales Doc Type'] = 'ZOR'
        added_dates['PO Number'] = ""
        added_dates['Order Reason'] = ""
    
        if ord_type == 'Draft':
            added_dates['Delivery Block'] = "ZP"
            added_dates['PO Type'] = "PRDT"
        elif ord_type == 'Force_Shipment':
            added_dates['Delivery Block'] = "\\"
            added_dates['PO Type'] = "FRSH"
    
        added_dates['Shipping Conditions'] = "\\"
        added_dates['Name of Order'] = ""
        added_dates['Your Reference'] = int(datetime.datetime.now().timestamp())
        added_dates['Sales Unit'] = "CS"
        added_dates['Item Category'] = ""
        added_dates['Useage Indicator'] = ""
        added_dates['Messages'] = ""
        added_dates['H/D'] = ""
        #added_dates = added_dates[(added_dates.DC.str.len() > 0)]
    
        #CONDITIONAL H/D COLUMN LOGIC
        clean_frame = added_dates[["H/D", "Sales Doc Type", "Sales Org", "DC", "Sold to", "PO Number", "Delivery Date", "Order Reason",
        "Delivery Block", "Shipping Conditions", "PO Type", "Name of Order", "Your Reference", "Material Number", "Order Quantity",
        "Sales Unit", "Item Category", "Useage Indicator", "Messages"]]

    except Exception as e:
        print("error in clean_added_dates {}".format(e))
    else:
        return clean_frame
        
# ################################################################################################################################################
# ################################################################################################################################################

# make some if statements with the ord_type
# This function cleans up the final dataframe with the condition that dates were given with the ingested blob data. AKA: The date column is already there.
# The function has so if statements in place depending on parameters passed with the blob input via the power app. Namely order type: (Draft or ForceShipment)
def clean_frame_normal(matblock_pass, ord_type):
    try:
        # matblock_pass = matblock_pass.drop(['Source', 'PackUPC', 'DATBI', 'DATAB', 'MaterialStatus', '_SCL_AUTH_MATLST', 'KSCHL', 'KAPPL', 'MANDT', '_SCL_TRADENAME', 'SMATN', 'MATWA', 'KUNNR', 'WERKS', 'DeterminationType', 'DeliveringPlant', 'Tradename', 'AuthMaterialListID', 'Name1', 'CentralOrderBlock', 'PODSDIndicator', 'SalesOffice'], axis = 1)
        matblock_pass = matblock_pass.rename(columns={"VKORG": "Sales Org"})
        matblock_pass = matblock_pass.rename(columns={"VTWEG": "DC"})
        matblock_pass = matblock_pass.rename(columns={"SuggestedCaseVolume": "Order Quantity"})
        matblock_pass = matblock_pass.rename(columns={"SMATN": "Material Number"})
        matblock_pass = matblock_pass.rename(columns={"CustomerNumber": "Sold to"})
        matblock_pass['Sales Doc Type'] = 'ZOR'
        matblock_pass['PO Number'] = ""
        matblock_pass['Order Reason'] = ""
        # Draft order if statements
        if ord_type == 'Draft':
            matblock_pass['Delivery Block'] = "ZP"
            matblock_pass['PO Type'] = "PRDT"
        elif ord_type == 'Force_Shipment':
            matblock_pass['Delivery Block'] = "\\"
            matblock_pass['PO Type'] = "FRSH"
        matblock_pass['Shipping Conditions'] = "\\"
        matblock_pass['Name of Order'] = ""
        matblock_pass['Your Reference'] = int(datetime.datetime.now().timestamp())
        matblock_pass['Sales Unit'] = "CS"
        matblock_pass['Item Category'] = ""
        matblock_pass['Useage Indicator'] = ""
        matblock_pass['Messages'] = ""
        matblock_pass['H/D'] = ""
    
        #CONDITIONAL H/D COLUMN LOGIC
        clean_frame = matblock_pass[["H/D", "Sales Doc Type", "Sales Org", "DC", "Sold to", "PO Number", "Delivery Date", "Order Reason",
        "Delivery Block", "Shipping Conditions", "PO Type", "Name of Order", "Your Reference", "Material Number", "Order Quantity",
        "Sales Unit", "Item Category", "Useage Indicator", "Messages"]]

    except Exception as e:
        print("error in clean_frame_normal {}".format(e))
    else:
        return clean_frame

# ################################################################################################################################################
# ################################################################################################################################################

def upload_utility(added_dates, ord_date, ord_type, file_name):
    try:
        #added_dates = added_dates.drop(['Source', 'PackUPC', 'DATBI', 'DATAB', 'MaterialStatus', '_SCL_AUTH_MATLST', 'KSCHL', 'KAPPL', 'MANDT', '_SCL_TRADENAME', 'SMATN', 'MATWA', 'KUNNR', 'WERKS', 'DeterminationType', 'DeliveringPlant', 'Tradename', 'AuthMaterialListID', 'Name1', 'CentralOrderBlock', 'PODSDIndicator', 'SalesOffice'], axis = 1)
        added_dates = added_dates.rename(columns={"VKORG": "SOrg"})
        added_dates = added_dates.rename(columns={"VTWEG": "DChl"})
        added_dates = added_dates.rename(columns={"SuggestedCaseVolume": "Order Quantity"})
        added_dates = added_dates.rename(columns={"SMATN": "Material"})
        added_dates = added_dates.rename(columns={"CustomerNumber": "Sold-To Pt"})

        added_dates = added_dates.rename(columns={"deliveryDate": "Req.dlv.dt"})
        added_dates['Req.dlv.dt'] = pd.to_datetime(added_dates['Req.dlv.dt'])
        # convert from non-null object type to a datetime object type so it can be formatted correctly

        added_dates['SaTy'] = "ZOR"
        added_dates['Purchase order no'] = ""
                
        added_dates['Dv'] = 'Z0'
        if ord_type == 'Draft':
            added_dates['DlBl'] = "ZP"
            added_dates['POtyp'] = 'PRDT'
        elif ord_type == 'Force_Shipment':
            added_dates['DlBl'] = ""
            added_dates['POtyp'] = 'FRSH'
        added_dates['Name of Order'] = ""
        added_dates['Your Reference'] = ""
        added_dates['SU'] = "CS"
        added_dates['RRC'] = ""

        added_dates['Doc. Date'] = ""
        #added_dates['Doc. Date'] = pd.to_datetime(added_dates['Doc. Date'])
        # convert from non-null object type to a datetime object type so it can be formatted correctly

        added_dates['Itenerary'] = file_name
        added_dates = added_dates[(added_dates.DChl.str.len() > 0)]
        #added_dates = added_dates.drop_duplicates(keep = 'first')
        added_dates['Item'] = 0
        _customer = added_dates["Sold-To Pt"][0]
        count = 0
        epoch = int(datetime.datetime.now().timestamp())
        for i, row in added_dates.iterrows():
            if _customer == added_dates.at[i, "Sold-To Pt"]:
                count = count + 10
            else:
                count = 10
                epoch = epoch + count
                _customer = added_dates.at[i, "Sold-To Pt"]
            added_dates.at[i,'Item'] = count
            added_dates.at[i, 'Your Reference'] = epoch


        #CONDITIONAL H/D COLUMN LOGIC
        sales_util = added_dates[["SaTy", "SOrg", "DChl", "Dv", "Purchase order no", "Your Reference", "Sold-To Pt", "Req.dlv.dt", "Item",
        "Material", "Order Quantity", "SU", "RRC", "POtyp", "DlBl", "Doc. Date", "Itenerary"]]

    except Exception as e:
        logger.error("Exception at upload_utility {}".format(e))
    
    else:
        return sales_util

# ################################################################################################################################################
# ################################################################################################################################################

# This function combines multiple other functions to execute the final cleaning of the DataFrame and dropping it back into blob storage.
# There is some business requirement logic for the H/D column put in place here to mark the beginning and continuation of orders via the letters "H" and "D"
# This piece assumes a date has not been given and incorporates the CONA API call to get the next delivery day.
def without_date(matblock_pass, file_name, ord_type, ord_date, ord_loc, ord_sys, landing):
    try:
        file_name =file_name.split(sep = '.')[0]
        added_dates = cona_api(ord_loc, file_name, ord_date, matblock_pass)
        if ord_sys == 'Winshuttle':
            clean_frame = clean_added_dates(added_dates, ord_type)
            # add logic to deal with one item scenario
            #clean_frame = clean_frame.sort_values(by=['Delivery Date', 'Sold to', 'Material Number'], ascending = True)
            clean_frame = clean_frame.sort_values(by=['Sold to'], ascending = True)
            clean_frame['H/D'] = clean_frame['Sold to'].ne(clean_frame['Sold to'].shift(1).bfill()).astype(int)
            clean_frame['H/D'] = clean_frame['H/D'].map({0: "D", 1: "H"})
            #clean_frame = clean_frame.drop_duplicates(keep = 'first')
            clean_frame.iloc[0,0] = "H"
            
            
            with tempfile.NamedTemporaryFile(prefix=GetValue('EXCEL_SHEETS', 'clearfile_prefix').format(file_name)) as f:
                workbook = xlsxwriter.Workbook(f.name + '.xlsx')
                writer = pd.ExcelWriter(f.name + '.xlsx', engine = 'xlsxwriter')
                clean_frame.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'cleared_data'), index = False)
                writer.save()
                workbook.close

                toblob(f.name + '.xlsx', landing)
        elif ord_sys == 'Upload_Utility':
            utility = upload_utility(added_dates, ord_date, ord_type, file_name)
            with tempfile.NamedTemporaryFile(prefix=GetValue('EXCEL_SHEETS', 'clearfile_prefix').format(file_name)) as f:
                # workbook = xlsxwriter.Workbook(f.name + '.xlsx')
                # writer = pd.ExcelWriter(f.name + '.xlsx', engine = 'xlsxwriter')
                utility = utility.sort_values(by=['Sold-To Pt', 'Item'], ascending = True)
                utility.to_csv(f.name + '.csv', date_format = '%Y%m%d', encoding = 'utf-8', index = False)
                # writer.save()
                # workbook.close

                toblob(f.name + '.csv', landing)

                #if ord_type == 'Force_Shipment':
                    #cnopts = pysftp.CnOpts()
                    #cnopts.hostkeys = ""
                    #cnopts.hostkeys = None
                    #with pysftp.Connection(host="",username="",password="",port = 22, cnopts=cnopts) as srv:
                    #with pysftp.Connection(
                    #    host= ""#GetValue('SFTP_CONNECTION', 'host')
                    #    ,username=""#GetValue('SFTP_CONNECTION', 'username')
                    #    ,password=""#GetValue('SFTP_CONNECTION', 'password')
                    #    ,port= 22 # GetValue('SFTP_CONNECTION', 'port')
                    #    ,cnopts=cnopts
                    #    ) as srv:
                        #print(srv.listdir(remotepath=''))

                        #srv.put(f.name + '.csv', '/PMN/OTC/IN/PredectiveSalesorder/' + 'united_' + datetime.datetime.now().timestamp() + '.csv') #upload file to nodejs/
                        #srv.put(f.name + '.csv', '/PMN/OTC/IN/PredectiveSalesorder/' + 'PRDtestDROP' + '.csv')
                        #srv.close()
    except Exception as e:
        logger.error("Exception at without_date {}".format(e))

# ################################################################################################################################################
# ################################################################################################################################################

# This function combines multiple other functions to execute the final cleaning of the DataFrame and dropping it back into blob storage.
# There is some business requirement logic for the H/D column put in place here to mark the beginning and continuation of orders via the letters "H" and "D"
# This piece will kick off if the date column is already there. Not calling the CONA API.
def with_date(matblock_pass, ord_type, landing, file_name):
    try:
        file_name =file_name.split(sep = '.')[0]
        clean_frame = clean_frame_normal(matblock_pass, ord_type)
        clean_frame = clean_frame.sort_values(by=['Delivery Date', 'Sold to', 'Material Number'], ascending = True)
        clean_frame['H/D'] = clean_frame['Sold to'].ne(clean_frame['Sold to'].shift(1).bfill()).astype(int)
        clean_frame['H/D'] = clean_frame['H/D'].map({0: "D", 1: "H"})
        #clean_frame = clean_frame.drop_duplicates(keep = 'first')
        clean_frame.iloc[0,0] = "H"
        with tempfile.NamedTemporaryFile(prefix=GetValue('EXCEL_SHEETS', 'clearfile_prefix').format(file_name)) as f:
            workbook = xlsxwriter.Workbook(f.name + '.xlsx')
            writer = pd.ExcelWriter(f.name + '.xlsx', engine = 'xlsxwriter')
            clean_frame.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'cleared_data'), index = False)
            writer.save()
            workbook.close

            toblob(f.name + '.xlsx', landing)
    except Exception as e:
        print("error in with_date {}".format(e))

# ################################################################################################################################################
# ################################################################################################################################################

# This function will generate the failed data from previous dataframe outputs.
# The business requested a multi-sheet excel book for each step of the check as the output.
def failed_data(no_exist, dfZ000_fail, dfZ002_fail_copy, failed_determination, indblock_fail, matblock_fail, matinv, landing, file_name):
    try:
        file_name =file_name.split(sep = '.')[0]
        nocus = no_exist
        soaml = dfZ000_fail
        cusaml = dfZ002_fail_copy
        determination = failed_determination
        blockPO = indblock_fail
        matblock = matblock_fail


        with tempfile.NamedTemporaryFile(prefix=GetValue('EXCEL_SHEETS', 'failfile_prefix').format(file_name)) as f:
            workbook = xlsxwriter.Workbook(f.name + '.xlsx')
            writer = pd.ExcelWriter(f.name + '.xlsx', engine = 'xlsxwriter')
            nocus.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'nocus'), index = False)
            determination.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'determination'), index=False)
            soaml.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'salesofficeaml'), index=False)
            cusaml.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'customeraml'), index=False)
            blockPO.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'blockPO'), index = False)
            matblock.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'matblock'), index = False)
            matinv.to_excel(writer, sheet_name=GetValue('EXCEL_SHEETS', 'inventory'), index = False)
            writer.save()
            workbook.close

            toblob(f.name + '.xlsx', landing)
    except Exception as e:
        print("error in failed_data {}".format(e))

# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################

# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################

# MAIN FUNCTION
if __name__ == '__main__':
    mypath = os.getcwd() + '\\config.ini'
    parser = ConfigParser()
    parser.read(mypath, encoding = 'utf-8')
    
    azureQueueAccountName = GetValue('QUEUE_STORAGE', 'queue_account_name')
    azureQueueKey = GetValue('QUEUE_STORAGE', 'queue_account_key')
    azureQueueParameters = GetValue('QUEUE_STORAGE', 'queue_name')
    
    ord_loc = 'corporate'
    landing = 'landing'

    queue_service = QueueService(account_name=azureQueueAccountName, account_key=azureQueueKey)

    appvars = read_next_in_queue()
    ord_type = appvars[0]
    ord_date = appvars[1]
    ord_mail = appvars[2]
    ord_sys = appvars[3]
    file_name = appvars[4]
    
    catastrophe = GetValue('LOGIC_APP_REQUESTS', 'catastrophe')
    invoke_email = GetValue('LOGIC_APP_REQUESTS', 'invoke_email')
    catastrophe = catastrophe.format(ord_mail, file_name)
    invoke_email = invoke_email.format(ord_mail)
    reset = {"FACTORY_RESET":["reset"]}
    factory = 'factory'
    
    tempdir = tempfile.mkdtemp()
    path = os.path.join(tempdir)
    
    try:
        with open(path + '\\reset.csv', 'a') as f:
            reset = pd.DataFrame(reset, columns = ["FACTORY_RESET"])
            reset.to_csv(f.name, encoding = "utf-8")
            toblob(f.name, factory)
        factory_reset(factory)
        infile = blob_read(ord_loc, file_name)
        cust_result = customer_data(infile)
        cust_result_pass = cust_result[0]
        no_exist = cust_result[1]
        cust_result_pass.loc[cust_result_pass['Tradename'] == '99999', 'Tradename'] = ""
        indblock_pass = cust_result_pass[(cust_result_pass.PODSDIndicator.str.contains("X") == False) & ((cust_result_pass.CentralOrderBlock.str.len() == 0) == True)]
        indblock_fail = cust_result_pass[(cust_result_pass.PODSDIndicator.str.contains("X") == True) | ((cust_result_pass.CentralOrderBlock.str.len() > 0) == True)]

        if 'MaterialNumber' in indblock_pass.columns:
            cust_pass = get_UPC(indblock_pass)
        else:
            pass

        determinate = determinate()
        
        determinate['MATWA'] = determinate['MATWA'].str.lstrip("0")
        determinate['KUNNR'] = determinate['KUNNR'].str.lstrip("0")
        determinate['SMATN'] = determinate['SMATN'].str.lstrip("0")


        nineten = nineten(cust_pass, determinate)
        failed_910 = nineten[1]
        
        ninefourteen = ninefourteen(failed_910, determinate)
        failed_914 = ninefourteen[1]
        del cust_pass
        
        nineseventeen = nineseventeen(failed_914, determinate)
        failed_917 = nineseventeen[1]
        del failed_910
        
        ninethirteen = ninethirteen(failed_917, determinate)
        failed_913 = ninethirteen[1]
        del failed_914
        
        sales_org = sales_org(failed_913, determinate)
        failed_determination = sales_org[1]
        del failed_917, determinate
        
        passed_det = pd.concat([nineten[0], ninefourteen[0], nineseventeen[0], ninethirteen[0], sales_org[0]], axis=0)
        
        
        del sales_org, nineten, ninefourteen, nineseventeen, ninethirteen

        Z002 = exclusionZ002()
        Z000 = exclusionZ000()
        Z000['MATNR'] = Z000['MATNR'].str.lstrip("0")
        Z002['MATNR'] = Z002['MATNR'].str.lstrip("0")
        dfZ000 = soaml(Z000, passed_det)
        dfZ002 = cusaml(Z002, dfZ000[0])
        

        df_all = pd.concat([dfZ000[0], dfZ002[0]], axis = 0)
        df_all = df_all.drop(['KSCHL', 'MANDT', 'KAPPL', '_SCL_AUTH_MATLST', 'MATNR', 'DATBI', 'DATAB'], axis = 1)
        dfZ000_fail = dfZ000[1]
        dfZ002_fail = dfZ002[1]
        dfZ002_fail_copy = dfZ002_fail.copy()
        dfZ002_fail = dfZ002_fail.drop(['MaterialNumber', 'SuggestedCaseVolume', 'SalesOffice', 'PODSDIndicator', 'CentralOrderBlock', 'Name1', 'AuthMaterialListID', 'Tradename', 'DeliveringPlant', 'PackUPC', 'DeterminationType', 'Source', 'KUNNR', 'MATWA', 'WERKS', 'VKORG', 'VTWEG'], axis = 1)
        dfZ002_fail = dfZ002_fail.rename(columns={"CustomerNumber": "CusNum"})
        dfZ002_fail = dfZ002_fail.rename(columns={"SMATN": "FailMat"})
        df_all = pd.merge(df_all, dfZ002_fail, how = 'left', left_on = ('CustomerNumber', 'MaterialNumber'), right_on = ('CusNum', 'FailMat'))
        df_all = df_all[(df_all.FailMat.str.len() > 0) == False]
        df_all = df_all.drop_duplicates(keep = 'first')
        matblocked = block_status()
        matblock = block_check(matblocked, df_all)
        matblock_pass = matblock[0]
        matblock_fail = matblock[1]
        matblock_copy = matblock[2]
        matinv = inventory(matblock_copy, matblocked)
        del matblocked, matblock, df_all

        if "Delivery Date" not in infile.columns:
            without_date(matblock_pass, file_name, ord_type, ord_date, ord_loc, ord_sys, landing)
            failed_data(no_exist, dfZ000_fail, dfZ002_fail_copy, failed_determination, indblock_fail, matblock_fail, matinv, landing, file_name)
        elif "Delivery Date" in infile.columns:
            with_date(matblock_pass, ord_type, ord_loc, file_name)
            failed_data(no_exist, dfZ000_fail, dfZ002_fail_copy, failed_determination, indblock_fail, matblock_fail, matinv, landing, file_name)
        blob_delete(ord_loc, file_name)
        requests.get(invoke_email)
    except Exception as e:
        print("exception in main: {}".format(e))
        requests.get(catastrophe)
# ################################################################################################################################################
# ################################################################################################################################################
# ################################################################################################################################################







