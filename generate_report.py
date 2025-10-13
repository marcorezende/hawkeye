from datetime import date, timedelta

from weasyprint import HTML

output_path = "generated_report.pdf"

name = "title"
today = date.today()
last_week = today - timedelta(days=7)

week_range = f"{last_week.strftime('%d/%m/%Y')} - {today.strftime('%d/%m/%Y')}"

with open("report.html", "r", encoding="utf-8") as f:
    html_template = f.read()

html_filled = html_template.format(name=name, date=today.strftime('%d/%m/%Y'), week_range=week_range)

HTML(string=html_filled, base_url=".").write_pdf(target=output_path)

