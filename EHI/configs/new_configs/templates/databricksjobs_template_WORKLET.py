import json
import requests

# Opening JSON file
f = open("%WORKLET_FILE%")
jsonFile = json.load(f)

DOMAIN = "%DATABRICKS_INSTANCE%" #'<databricks-instance>'
TOKEN = "%DATABRICKS_TOKEN%" #'<your-token>'

response = requests.post(
  'https://%s/api/2.1/jobs/run-now' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  json=jsonFile
)

if response.status_code == 200:
    print(response.json())
else:
    print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))