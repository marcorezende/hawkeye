import os
import time
from datetime import datetime, date, timedelta
from pprint import pprint
from urllib.parse import urlparse, urlunparse

import boto3
import duckdb
import requests
import json

from openai import OpenAI

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

    def run(self, chart_id, name):
        chart_cache_screenshot_url = self.cache_screenshot_url.format(id=chart_id)
        headers = self.get_auth_token(url=self.base_url, username=self.username, password=self.password)
        image_url = self.cache_screenshot(cache_screenshot_url=chart_cache_screenshot_url, headers=headers)
        new_image_url = self.change_base_url(old_url=image_url, new_base_url=self.base_url)
        self.download_screenshot(url=new_image_url, headers=headers, output_path=f"./data/img/{name}.png")


def generate_report_pipe(company, start_date, end_date, report_name):
    BASE_URL = "http://superset:8088"
    USERNAME = os.getenv('SUPERSET_USERNAME')
    PASSWORD = os.getenv('SUPERSET_PASSWORD')

    charts_mapping = {
        'total_vencidos': 8,
        'total_visitas': 9,
        'total_nao_conformidades': 10,
        'media_nota': 11,
        'conformidade_por_unidade': 12,
        'media_nota_por_unidade': 13,
        'porcentagem_conformidade': 14,
        'conformidade_por_area': 15,
        'itens_nao_conformes': 16,
        'vencidor_por_loja': 17

    }

    chart_ids = charts_mapping.values()

    UpdateChart(BASE_URL, USERNAME, PASSWORD, chart_ids, company).run()

    for k, chart_id in charts_mapping.items():
        ScreenshotChart(BASE_URL, USERNAME, PASSWORD).run(chart_id=chart_id, name=k)

    duckdb.sql(f"""
    CREATE OR REPLACE PERSISTENT SECRET my_secret (
    TYPE S3,
    REGION 'us-east-1',
    KEY_ID '{os.getenv("MINIO_ACCESS_KEY")}',
    SECRET '{os.getenv("MINIO_SECRET_KEY")}',
    ENDPOINT '{os.getenv("MINIO_ENDPOINT").replace('http://', '')}',
    USE_SSL 'false',
    URL_STYLE 'path');
    """)

    text = duckdb.sql(f"""
    SELECT DISTINCT final_comments
    FROM 's3://hawkeye/lm/cleaned/checklist.parquet'
    WHERE unidade = '{company}' and data_inicial BETWEEN '{str(start_date)}' AND '{str(end_date)}'
    ORDER BY data_inicial DESC
    """).df().to_markdown()

    max_chars = 28_000 * 4

    if len(text) > max_chars:
        print(f"[INFO] Texto truncado de {len(text)} para {max_chars} caracteres.")
        text = text[:max_chars]

    template = f'''
    You are a food safety and data analyst specialist to LM food safety consultant.
    LM Segurança Alimentar is a brazilian company that give consultant to restaurants and supermarkets about food safety.
    It conducts monitoring, audits, and training to ensure that products are stored, handled, and displayed in accordance
    with health regulations. In addition, it verifies temperatures, expiration dates, and the hygiene of equipment
    and employees, preventing contamination and reducing health risks to consumers.
    Your task is to analyse previous visits to establishments and give summarized version to the client.

    # Instructions:
    1. Synthesize key findings into a cohesive narrative (max 100 words)
    2. Highlight recurring issues or patterns across visits
    3. Write in paragraph format only - no lists or bullet points
    4. Use Brazilian Portuguese (pt-BR)
    5. Focus on actionable insights: non-compliances, critical temperature deviations, hygiene issues, and expired products
    6. Do not add conclusions, recommendations, or extra commentary beyond the data summary

    # Guardrails:
    - ONLY analyze data provided in the Visit Data section below
    - DO NOT invent, assume, or extrapolate information not present in the data
    - Maintain professional, neutral tone - avoid alarmist or dismissive language

    # Visit Date Range:
    Start date: {{start_date}}
    End date: {{end_date}}
    
    # Visit Data:
    {{dataframe}}

    Output the summary in pt-BR as a single paragraph.
    '''

    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"))
    response = client.chat.completions.create(
        model='gpt-4o',
        messages=[
            {"role": "user", "content": template.format(dataframe=text,
                                                        start_date=str(start_date), end_date=str(end_date))},
        ],
        temperature=0.2
    )

    generate_report_pdf(output_path='report.pdf', name=company, text=response.choices[0].message.content,
                        start_date=start_date, end_date=end_date)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
        region_name="us-east-1",
    )

    s3.upload_file('report.pdf', os.getenv('MINIO_BUCKET'), f'lm/reports/{report_name}')
