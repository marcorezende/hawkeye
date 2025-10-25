import boto3
from playwright.sync_api import sync_playwright
import time
import os
from dotenv import load_dotenv


def ingest_data():
    load_dotenv()
    username = os.getenv("CHECKLIST_FACIL_USERNAME")
    password = os.getenv("CHECKLIST_FACIL_PASSWORD")
    download_dir = "./downloads"
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto("https://spa.checklistfacil.com.br/login?lang=pt-br")
        page.wait_for_selector("#mat-input-1")

        page.fill("#mat-input-1", username)
        page.click("button:has-text('Continuar')")
        page.wait_for_selector("#mat-input-0")

        page.fill("#mat-input-0", password)
        page.click("button:has-text('Entrar')")

        page.wait_for_url("https://app.checklistfacil.com.br/**", timeout=60000)
        page.goto("https://app.checklistfacil.com.br/evaluations")
        page.wait_for_selector("#start_date input.mdc-text-field__input")

        page.fill("#start_date input.mdc-text-field__input", "17/10/2025")
        page.keyboard.press("Tab")

        page.fill("#end_date input.mdc-text-field__input", "25/10/2025")
        page.keyboard.press("Tab")

        page.click("button:has-text('Filtrar') >> text='Filtrar'")
        time.sleep(30)

        page.click("#button-bulk-export")
        page.get_by_role("radio", name="CSV").check()
        page.locator("#export_bulk_evaluation_type_csv").get_by_role("textbox").click()
        page.locator('li[data-value="evaluation_row_items_csv"]:visible').click()
        page.get_by_role("button", name="Exportar").click()
        time.sleep(30)
        page.get_by_role("link", name="import_export").click()
        time.sleep(20)

        first_row = page.locator("table.data-table tbody tr:first-child")

        first_row.hover()

        page.wait_for_selector("table.data-table tbody tr:first-child a.js-row-download-button:visible")

        first_download_button = page.locator("table.data-table tbody tr:first-child a.js-row-download-button:visible")

        with page.expect_download() as download_info:
            first_download_button.click()

        download = download_info.value
        download_path = os.path.join(download_dir, download.suggested_filename)
        download.save_as(download_path)
        print("Arquivo baixado para:", download.path())

        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv('MINIO_ENDPOINT'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
            region_name="us-east-1",
        )
        #
        local_file_path = download_path

        object_name = os.path.basename(local_file_path)

        s3_client.upload_file(local_file_path, os.getenv('MINIO_BUCKET'), f'landing/lm/{object_name}')
        print(f"Arquivo enviado para MinIO: s3://{os.getenv('MINIO_BUCKET')}/landing/lm/{object_name}")

