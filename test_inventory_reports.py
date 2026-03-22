import config
import time
from sp_api.api import Reports
from sp_api.base import Marketplaces, ReportType

creds = {
    'refresh_token': config.AMAZON_REFRESH_TOKEN,
    'lwa_app_id': config.AMAZON_CLIENT_ID,
    'lwa_client_secret': config.AMAZON_CLIENT_SECRET,
}
api = Reports(credentials=creds, marketplace=Marketplaces.US)

report_types = [
    'GET_FBA_INVENTORY_PLANNING_DATA',
    'GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA',
    'GET_FBA_FULFILLMENT_INVENTORY_SUMMARY_DATA',
    'GET_FBA_MYI_ALL_INVENTORY_DATA',
]

for rt in report_types:
    print("Testing:", rt)
    r = api.create_report(reportType=rt, marketplaceIds=[config.US_MARKETPLACE_ID])
    rid = r.payload['reportId']
    print("  Report ID:", rid)
    time.sleep(35)
    s = api.get_report(reportId=rid)
    status = s.payload.get('processingStatus')
    print("  Status:", status)
    print()
    time.sleep(10)

print("Done.")
