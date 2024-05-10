import os
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import boto3

def lambda_handler(event, context):
  place_id = event['place']['id']

  host = os.environ['OPENSEACH_ENDPOINT']
  region = os.environ['REGION']
  service = 'es'
  credentials = boto3.Session().get_credentials()
  auth = AWSV4SignerAuth(credentials, region, service)

  open_search_client = OpenSearch(
      hosts = [{ 'host': host, 'port': 443 }],
      http_auth = auth,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection,
      pool_maxsize = 20
  )

  open_search_client.delete_by_query(
    index = 'events',
    body = {
      'query': {
        'match': {
          'placeId': place_id,
        },
      },
    },
  )

  return { 'message': f'Deleted events for place {place_id}' }