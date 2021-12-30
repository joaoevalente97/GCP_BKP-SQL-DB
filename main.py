import base64
import logging
import json
import pytz

from datetime import datetime
from httplib2 import Http

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

def main(event, context):
    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    credentials = GoogleCredentials.get_application_default()

    service = discovery.build('sqladmin', 'v1beta4', http=credentials.authorize(Http()), cache_discovery=False)

    # Formatar datestamp: YearMonthDayHourMinute = %Y%m%d%H%M
    # pytz.timezone define o fuso horario
    datestamp = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y%m%d") 

    #Define o nome do arquivo como %data%%banco%.sql.gz
    uri = "{0}/{1}{2}.sql.gz".format(pubsub_message['gs'], datestamp, pubsub_message['db']) 
    
    instances_export_request_body = {
      "exportContext": {
        "kind": "sql#exportContext",
        "fileType": "SQL",
        "uri": uri,
        "databases": [
          pubsub_message['db']
        ]
      }
    }

    try:
      request = service.instances().export(
            project=pubsub_message['project'],
            instance=pubsub_message['instance'],
            body=instances_export_request_body
        )
      response = request.execute()
    except HttpError as err:
        logging.error("Could NOT run backup. Reason: {}".format(err))
    else:
      logging.info("Backup task status: {}".format(response))