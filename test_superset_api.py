from pprint import pprint

import requests

BASE_URL = "http://localhost:8088/api/v1"

LOGIN_URL = f"{BASE_URL}/security/login"
payload = {
    "username": "admin",
    "password": "admin",
    "provider": "db"
}

response = requests.post(LOGIN_URL, json=payload)
access_token = response.json()["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}

print(access_token)

r = requests.get(f"{BASE_URL}/dashboard/", headers=headers)
dashboards = r.json()
pprint(dashboards)

charts_resp = requests.get(f"{BASE_URL}/chart/", headers=headers)
charts_resp.raise_for_status()

charts_data = charts_resp.json()
for chart in charts_data["result"]:
    print(f" - {chart['id']}: {chart['slice_name']} ({chart['viz_type']})")