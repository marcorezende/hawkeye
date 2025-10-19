import time
from pprint import pprint
from urllib.parse import urlparse, urlunparse

import requests
import json

from weasyprint import HTML

BASE_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"


class UpdateChart:
    def __init__(self, base_url, username, password, charts, company):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.charts = charts
        self.company = company

    @staticmethod
    def get_auth_token(url, username, password):
        login_resp = requests.post(f"{url}/api/v1/security/login", json={
            "username": username,
            "password": password,
            "provider": "db"
        })
        access_token = login_resp.json()["access_token"]
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def get_chart(chart_id, headers, url):
        chart_resp = requests.get(f"{url}/api/v1/chart/{chart_id}", headers=headers)
        chart_data = chart_resp.json()["result"]
        params = json.loads(chart_data["params"])
        return params, chart_data

    @staticmethod
    def add_filter(chart_filter, company):
        new_filter = chart_filter.copy()
        new_filter = list(filter(lambda x: x['subject'] != 'unidade', new_filter))
        new_filter.append({
            "expressionType": "SIMPLE",
            "subject": "unidade",
            "operator": "IN",
            "comparator": [company],
            "clause": "WHERE",
            "sqlExpression": None
        })
        return new_filter

    @staticmethod
    def update_chart(chart_id, params, chart_data, headers, url):
        update_payload = {
            "slice_name": chart_data["slice_name"],
            "viz_type": chart_data["viz_type"],
            "datasource_type": "table",
            "datasource_id": json.loads(chart_data["query_context"])["datasource"]["id"],
            "params": json.dumps(params)
        }
        pprint(update_payload)
        put_resp = requests.put(f"{url}/api/v1/chart/{chart_id}", headers={**headers,
                                                                           "Content-Type": "application/json"},
                                json=update_payload)
        if put_resp.status_code == 200:
            put_resp.json()
        else:
            raise Exception(f"Error: {put_resp.status_code}: {put_resp.text}")

    def run(self):
        headers = self.get_auth_token(url=self.base_url, username=self.username, password=self.password)
        for chart_id in self.charts:
            params, chart_data = self.get_chart(chart_id=chart_id, headers=headers, url=self.base_url)
            new_filter = self.add_filter(chart_filter=params["adhoc_filters"], company=self.company)
            params["adhoc_filters"] = new_filter
            self.update_chart(chart_id=chart_id,
                              params=params,
                              chart_data=chart_data,
                              headers=headers,
                              url=self.base_url)


class ScreenshotChart:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.cache_screenshot_url = self.base_url + "/api/v1/chart/{id}/cache_screenshot/"

    @staticmethod
    def get_auth_token(url, username, password):
        login_resp = requests.post(f"{url}/api/v1/security/login", json={
            "username": username,
            "password": password,
            "provider": "db"
        })
        access_token = login_resp.json()["access_token"]
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    def change_base_url(old_url, new_base_url):
        parsed_old = urlparse(old_url)
        parsed_new_base = urlparse(new_base_url)

        new_url = urlunparse((
            parsed_new_base.scheme or parsed_old.scheme,
            parsed_new_base.netloc or parsed_old.netloc,
            parsed_old.path,
            parsed_old.params,
            parsed_old.query,
            parsed_old.fragment
        ))

        return new_url

    @staticmethod
    def cache_screenshot(cache_screenshot_url, headers):
        response = requests.get(cache_screenshot_url, headers=headers)
        if response.status_code == 200 or response.status_code == 202:
            data = response.json()["image_url"]
            time.sleep(30)
            return data
        else:
            raise Exception(f"Error: {response.status_code}: {response.text}")

    @staticmethod
    def download_screenshot(url, headers, output_path):
        response = requests.get(url, headers={**headers})
        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Imagem salva em: {output_path}")
        else:
            raise Exception(f"Erro ao baixar imagem. Status: {response.status_code}, Detalhes: {response.text}")

    def run(self, chart_id):
        chart_cache_screenshot_url = self.cache_screenshot_url.format(id=chart_id)
        headers = self.get_auth_token(url=self.base_url, username=self.username, password=self.password)
        image_url = self.cache_screenshot(cache_screenshot_url=chart_cache_screenshot_url, headers=headers)
        new_image_url = self.change_base_url(old_url=image_url, new_base_url=self.base_url)
        self.download_screenshot(url=new_image_url, headers=headers, output_path=f"./data/img{chart_id}.png")


UpdateChart(BASE_URL, USERNAME, PASSWORD, [3], 'SUPERMERCADO X').run()
# ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=1)
# ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=2)
ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=3)
# ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=4)
# ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=5)
# ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=6)
# ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=7)
# output_path = "generated_real_report.pdf"
# HTML("cv.html").write_pdf(target=output_path)
