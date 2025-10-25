from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/marcorezende/hawkeye.git",
        entrypoint="full_pipeline.py:fluxo_principal"
    ).deploy(
        name="lm-etl",
        work_pool_name="lm",
    )
