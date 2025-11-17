import os

import duckdb
from prefect import flow, task

from download_data import ingest_data


@task(log_prints=True)
def ingest():
    ingest_data()


@task
def create_secret():
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


@task
def raw():
    duckdb.sql("""
    COPY (
         SELECT
        "Código da avaliação",
        Unidade,
        Cidade,
        "Região/Grupo",
        "Nome do checklist",
        Autor,
        "Área",
        Item,
        Resposta,
        Imagens,
        "Data inicial",
        "Data final",
        "Data de sincronização",
        Resultado,
        "Comentários finais"
        FROM 's3://hawkeye/lm/landing/*.csv'
    ) TO 's3://hawkeye/lm/raw/checklist.parquet' (FORMAT 'parquet');
    """)


@task
def cleaned():
    duckdb.sql("""
    COPY (
    WITH raw AS (
         SELECT
        "Código da avaliação" as id,
        Unidade as unidade,
        Cidade as cidade,
        "Região/Grupo" as regiao,
        "Nome do checklist" as nome,
        Autor as autor,
        "Área" as area,
        Item as item,
        Resposta as reposta,
        Imagens as imagens,
        "Data inicial" as data_inicial,
        "Data final" as data_final,
        "Data de sincronização" as data_sincronizacao,
        Resultado AS result,
        "Comentários finais" as final_comments
       FROM  "s3://hawkeye/lm/raw/checklist.parquet"
    ), cleaned as (
        SELECT
        id,
        unidade,
        COALESCE(cidade, 'Não definido') as cidade,
        COALESCE(regiao, 'Não definido') as regiao,
        nome,
        autor,
        area,
        item,
        reposta,
        COALESCE(len(string_split(imagens, ' ')), 0) as total_fotos,
        TRY_CAST(reposta AS INTEGER) as total_vencidos,
        CASE WHEN item = 'Ausência de Produtos Vencidos nos expositores auditados' THEN true ELSE false END AS item_total_vencido,
        date_diff('minutes', data_inicial, data_final) as duracao,
        CAST(REPLACE(result, ',', '.') AS DOUBLE) AS result,
        data_inicial,
        data_final,
        data_sincronizacao,
        final_comments,
        FROM raw
    )
    SELECT DISTINCT id, unidade, cidade, regiao, nome, autor, area, item, reposta, total_fotos, duracao, result, data_inicial, data_final, data_sincronizacao, final_comments
    FROM cleaned
    ) TO 's3://hawkeye/lm/cleaned/checklist.parquet' (FORMAT 'parquet');
    """)


@flow
def fluxo_principal():
    ingest()
    create_secret()
    raw()
    cleaned()


if __name__ == "__main__":
    fluxo_principal()
