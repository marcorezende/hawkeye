from datetime import date

from weasyprint import HTML


def generate_report_pdf(output_path, name, text, start_date, end_date):
    day_range = f"{start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"
    today = date.today()
    with open("report.html", "r", encoding="utf-8") as f:
        html_template = f.read()

    html_filled = html_template.format(name=name, date=today.strftime('%d/%m/%Y'), week_range=day_range, summary=text)

    HTML(string=html_filled, base_url=".").write_pdf(target=output_path)
