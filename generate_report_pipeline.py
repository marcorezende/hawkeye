from prefect import flow

from superset_client import generate_report_pipe


@flow
def fluxo_principal(company, start_date, end_date, report_name):
    generate_report_pipe(company, start_date, end_date, report_name)


if __name__ == "__main__":
    fluxo_principal()
