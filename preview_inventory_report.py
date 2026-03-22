import config
import time
import gzip
import urllib.request
from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType

creds = {
    'refresh_token': config.AMAZON_REFRESH_TOKEN,
    'lwa_app_id': config.AMAZON_CLIENT_ID,
    'lwa_client_secret': config.AMAZON_CLIENT_SECRET,
}
api = Reports(credentials=creds, marketplace=Marketplaces.US)

print("Requesting GET_FBA_INVENTORY_PLANNING_DATA...")
r = api.create_report(
    reportType=ReportType.GET_FBA_INVENTORY_PLANNING_DATA,
    marketplaceIds=[config.US_MARKETPLACE_ID]
)
report_id = r.payload['reportId']
print("Report ID:", report_id)

time.sleep(35)
status = api.get_report(reportId=report_id)
print("Status:", status.payload.get('processingStatus'))

document_id = status.payload['reportDocumentId']
doc = api.get_report_document(reportDocumentId=document_id)
url = doc.payload['url']
compression = doc.payload.get('compressionAlgorithm')

with urllib.request.urlopen(url) as response:
    raw = response.read()

if compression == 'GZIP':
    content = gzip.decompress(raw).decode('utf-8', errors='replace')
else:
    content = raw.decode('utf-8', errors='replace')

lines = content.split('\n')
print("\nTotal lines:", len(lines))
print("\nHeaders:")
print(lines[0])
print("\nFirst data row:")
if len(lines) > 1:
    print(lines[1])
print("\nSecond data row:")
if len(lines) > 2:
    print(lines[2])
