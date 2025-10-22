from playwright.sync_api import sync_playwright
import time
import os
from dotenv import load_dotenv


def main():
    load_dotenv()
    username = os.getenv("CHECKLIST_FACIL_USERNAME")
    password = os.getenv("CHECKLIST_FACIL_PASSWORD")
    download_dir = "./downloads"
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto("https://spa.checklistfacil.com.br/login?lang=pt-br")
        page.wait_for_selector("#mat-input-1")

        page.fill("#mat-input-1", username)
        page.click("button:has-text('Continuar')")
        page.wait_for_selector("#mat-input-0")

        page.fill("#mat-input-0", password)
        page.click("button:has-text('Entrar')")

        time.sleep(10)
        page.goto("https://app.checklistfacil.com.br/evaluations")
        page.wait_for_selector("#start_date input.mdc-text-field__input")

        page.fill("#start_date input.mdc-text-field__input", "21/09/2025")
        page.fill("#end_date input.mdc-text-field__input", "28/09/2025")
        page.click("button:has-text('Filtrar')")

        time.sleep(5)


if __name__ == "__main__":
    main()
