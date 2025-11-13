from prefect import flow
from prefect import variables

from superset_client import generate_report_pipe


@flow
def fluxo_principal():
    # generate_report_pipe()
    print(variables.get("start_date"))


if __name__ == "__main__":
    fluxo_principal()
