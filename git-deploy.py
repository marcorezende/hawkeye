from prefect.client.schemas.schedules import CronSchedule
from prefect import flow

flow.from_source(
    source="https://github.com/marcorezende/hawkeye.git",
    entrypoint="full_pipeline.py:fluxo_principal"
).deploy(
    name="lm-etl",
    work_pool_name="lm",
    schedule=CronSchedule(
        cron="0 0 * * *",
        timezone="UTC"
    )
)

flow.from_source(
    source="https://github.com/marcorezende/hawkeye.git",
    entrypoint="generate_report_pipeline.py:fluxo_principal"
).deploy(
    name="lm-report",
    work_pool_name="lm"
)
