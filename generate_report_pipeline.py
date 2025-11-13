from prefect import flow

from superset_client import generate_report_pipe


@flow
def fluxo_principal(company, start_date, end_date):
    generate_report_pipe(company, start_date, end_date)


if __name__ == "__main__":
    fluxo_principal()
