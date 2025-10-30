import os
import time
from datetime import datetime
from pprint import pprint
from urllib.parse import urlparse, urlunparse

import boto3
import requests
import json

from generate_report import generate_report_pdf


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
        query_context = json.loads(chart_data["query_context"])
        return params, chart_data, query_context

    @staticmethod
    def add_filter(chart_filter, company):
        new_filter = chart_filter.copy()
        new_filter = list(filter(lambda x: x['subject'] not in ['unidade', 'data_inicial'], new_filter))
        new_filter.append(
            {'clause': 'WHERE',
             'comparator': 'Last week',
             'datasourceWarning': False,
             'expressionType': 'SIMPLE',
             'isExtra': False, 'isNew': False,
             'operator': 'TEMPORAL_RANGE',
             'sqlExpression': None,
             'subject': 'data_inicial'}
        )
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
            params, chart_data, query_context = self.get_chart(chart_id=chart_id, headers=headers, url=self.base_url)
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
        self.download_screenshot(url=new_image_url, headers=headers, output_path=f"./data/img/{chart_id}.png")


def generate_report_pipe():
    BASE_URL = "http://superset:8088"
    USERNAME = "admin"
    PASSWORD = "admin"

    charts_mapping = {
        'total_vencidos': 5,
        'total_visitas': 3,
        'total_nao_conformidades': 4,
        'media_nota': 6,
        'conformidade_por_unidade': 1,
        'media_nota_por_unidade': 7,
        'porcentagem_conformidade': 2,
        'conformidade_por_area': 9,
        'itens_nao_conformes': 8
    }

    chart_ids = charts_mapping.values()

    company = 'SUPERMERCADO RODRIGUES'

    UpdateChart(BASE_URL, USERNAME, PASSWORD, chart_ids, company).run()

    for chart_id in chart_ids:
        ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=chart_id)

    generate_report_pdf(output_path='report.pdf', name=company)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        region_name="us-east-1",
    )
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    object_name = f"{company.lower()}_report_{timestamp}.pdf"

    s3.upload_file('report.pdf', os.getenv('MINIO_BUCKET'), f'lm/reports/{object_name}')
