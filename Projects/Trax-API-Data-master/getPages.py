import requests
import json
import datetime
from azure.storage.queue import QueueService



def page_parse(dateFrom, dateTo):
    
    if not dateFrom:
        raise ValueError("page_parse dateFrom needs to be passed")

    if not dateTo:
        raise ValueError("page_parse dateTo needs to be passed")
        
    page_list = []


    #TODO .999999 is to account for milliseconds after 59 second
    #dateFrom = change_datetime_to_epoch(dateFrom)

    #TODO .999999 is to account for milliseconds after 59 second
    #dateTo = change_datetime_to_epoch(dateTo)


    base_url = 'https://services.traxretail.com/api/V4/projectname/analysis-results?from={}&to={}&page=0&per_page=200'.format(dateFrom, dateTo)
    base = "https://services.traxretail.com"
    apikey = "Auth-Token INSERT KEY HERE"
    headers = {"Authorization": apikey}
    r = requests.get(base_url, headers=headers)

    page_count = r.json()['metadata']['page_count']
    lastpage = page_count

    i = range(0, int(lastpage))
    for num in i:
        link = base + '/api/V4/projectname/analysis-results?from={}&to={}&page={}&per_page=200'.format(dateFrom, dateTo, num)
        page_list.append(link)
    return page_list

def put_in_queue(page_list):
    for page in page_list:
        queue_service.put_message(azureQueueAnalysisResults, "{}".format(page), time_to_live=-1)


if __name__ == '__main__':
    azureQueueAccountName = "traxwebjobs"
    azureQueueKey = "storage account key"
    azureQueueAnalysisResults = "analysis-results"

    queue_service = QueueService(account_name=azureQueueAccountName, account_key=azureQueueKey)

    #create queue if doesnt exist
    if not queue_service.exists(azureQueueAnalysisResults):
        queue_service.create_queue(azureQueueAnalysisResults)
    try:
        dateFrom = datetime.datetime.now().timestamp() - 86400
        dateTo = datetime.datetime.now().timestamp()
        page_list = page_parse(dateFrom, dateTo)

        put_in_queue(page_list)

    except Exception as e:
        pass
