import hashlib
import json
import os
import time
from datetime import datetime, date
from threading import Thread

import boto3
import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

DB_CONFIG = {
    'dbname': 'portal',
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
    'host': 'localhost',
    'port': os.getenv('POSTGRES_PORT', '5432')
}

PREFECT_API_URL = os.getenv('PREFECT_API_URL', 'http://localhost:4200/api')
PREFECT_API_AUTH_STRING = os.getenv('PREFECT_API_AUTH_STRING', 'http://localhost:4200/api')
PREFECT_USERNAME = PREFECT_API_AUTH_STRING.split(':')[0]
PREFECT_PASSWORD = PREFECT_API_AUTH_STRING.split(':')[1]
# Configurações do S3
MINIO_ENDPOINT = 'http://localhost:9000'
S3_BUCKET = os.getenv('MINIO_BUCKET')
S3_REGION = os.getenv('AWS_REGION')
AWS_ACCESS_KEY_ID = os.getenv('MINIO_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('MINIO_SECRET_KEY')


def get_s3_client():
    return boto3.client(
        's3',
        region_name=S3_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=MINIO_ENDPOINT
    )


def download_report_from_s3(file_path):
    full_path = f"lm/reports/{file_path}"
    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=full_path)
        return response['Body'].read()
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"Arquivo não encontrado no S3: {file_path}")
        else:
            print(f"Erro ao baixar arquivo do S3: {e}")
        return None
    except Exception as e:
        print(f"Erro inesperado ao baixar do S3: {e}")
        return None


def check_file_exists_in_s3(file_path):
    full_path = f"lm/reports/{file_path}"

    try:
        s3_client = get_s3_client()
        s3_client.head_object(Bucket=S3_BUCKET, Key=full_path)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            print(f"Erro ao verificar arquivo no S3: {e}")
            return False
    except Exception as e:
        print(f"Erro inesperado ao verificar arquivo no S3: {e}")
        return False


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255) UNIQUE,
        password_hash VARCHAR(255),
        role VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS company (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE,
        address TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS reports (
        id SERIAL PRIMARY KEY,
        company_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        start_date DATE,
        end_date DATE,
        file_path VARCHAR(500),
        status VARCHAR(50),
        flow_run_id VARCHAR(255),
        generated_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (company_id) REFERENCES company(id)
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS audit_logs (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        action VARCHAR(100),
        target_id INTEGER,
        details JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )''')

    # Migração: Adiciona coluna flow_run_id se não existir
    try:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='reports' AND column_name='flow_run_id'
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE reports ADD COLUMN flow_run_id VARCHAR(255)")
            print("Coluna flow_run_id adicionada com sucesso!")
            conn.commit()
    except Exception as e:
        print(f"Aviso na migração: {e}")
        conn.rollback()

    cur.execute("SELECT * FROM users WHERE email = 'admin@company.com'")
    if not cur.fetchone():
        password = hashlib.sha256('admin123'.encode()).hexdigest()
        cur.execute("INSERT INTO users (name, email, password_hash, role) VALUES (%s, %s, %s, %s)",
                    ('Admin User', 'admin@company.com', password, 'admin'))

    companies = [
        'SOHO LOUNGE',
        'Supermercado Cezar',
        'GUSTA +',
        'Padaria Barcelona',
        'PEIXE AMAZONICO',
        'Vitoria Supermercado',
        'Supermercado Meta',
        'Nonno Cozinha Autoral',
        'SUPERMERCADO COEMA',
        'Juma Mercado Express',
        'RESTAURANTE',
        'Jota Burguer',
        'Panificadora Leste Pan',
        'O MAQUINISTA',
        'Metazon/Moss',
        'Padaria Nobre',
        'REI DO CHURRASCO',
        'SUPERMERCADO GOIANA',
        'Brazin',
        'Supermercado Xavier',
        'Padaria Rio Tinto',
        'Mindu Burger',
        'Adolpho Shopping',
        'Adolpho Restaurante',
        'Colizeu Pizza',
        'Palhoça',
        'Adolpho Delivery',
        'FPF',
        'MESTRE PÃO P.10',
        'Cali Sushi',
        'RESTAURANTE CABOCLO',
        'Gima Bar',
        'SAN PAOLO',
        'Ramalhete',
        'FRANCOS PIZZA',
        'Supermercado Peres 02',
        'HOTEL RAMADA',
        'Rodrigues Colchões',
        'Panificadora Modelinho',
        'Bento Sorvetes',
        'Supermercado Peres 01',
        'Kin',
        'SEU LUIS',
        'Estaleiro Rio Amazonas ERAM',
        'SUPERMERCADO VIDAL',
        'Ni Hachi',
        'Hamburgella',
        'COQUEIRO VERDE',
        'PADARIA JASMYN',
        'Sorveteira Kamby',
        'SUPERMERCADO RODRIGUES',
        'Hotel TRYP',
        'Tortas & Tortas',
        'CN SUPERMERCADOS',
        'TREINAMENTO INTEGRAÇÃO',
        'ATACK',
        'Mestre do Pão',
        'Adão e Eva',
        'Kalena Café',
        'Supermercado Rio Negro',
        'SUPERMERCADO VENEZA',
        'TAYCHI SUSHI',
        'Torres Express',
        'Requintes Pães e Tortas',
        'PADARIA PÃO E VERSO',
        'Cafe da Terra',
        'Padaria Lisboa',
        'Tokay Sushi'
    ]

    for c in companies:
        cur.execute("""
            INSERT INTO company (name, address)
            VALUES (%s, NULL)
            ON CONFLICT (name) DO NOTHING
        """, (c,))

    conn.commit()
    cur.close()
    conn.close()


def log_audit(user_id, action, target_id=None, details=None):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO audit_logs (user_id, action, target_id, details) VALUES (%s, %s, %s, %s)",
                (user_id, action, target_id, json.dumps(details) if details else None))
    conn.commit()
    cur.close()
    conn.close()


def trigger_prefect_flow(parameters):
    try:
        headers = {
            'Content-Type': 'application/json'
        }

        url = f"{PREFECT_API_URL}/flow_runs/"

        payload = {
            'parameters': parameters,
            'flow_id': '659daac6-5995-4404-ad70-27608e266826',
            "deployment_id": "ca6f535b-071c-43a4-b6a3-937a7b241182",
            "work_pool_name": "lm",
            "state": {
                "type": "SCHEDULED"
            }
        }

        response = requests.post(url, json=payload, headers=headers, auth=(PREFECT_USERNAME, PREFECT_PASSWORD),
                                 timeout=10)
        response.raise_for_status()

        result = response.json()
        return {
            'success': True,
            'flow_run_id': result.get('id'),
            'status': result.get('state', {}).get('type'),
            'message': 'Flow acionado com sucesso'
        }

    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Falha ao acionar o flow Prefect: {str(e)}'
        }


def check_flow_run_status(flow_run_id):
    try:
        headers = {
            'Content-Type': 'application/json'
        }

        url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}"

        response = requests.get(url, headers=headers, auth=(PREFECT_USERNAME, PREFECT_PASSWORD), timeout=10)
        response.raise_for_status()

        result = response.json()
        return {
            'success': True,
            'status': result.get('state', {}).get('type'),
            'name': result.get('name'),
            'start_time': result.get('start_time'),
            'end_time': result.get('end_time')
        }

    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'error': str(e)
        }


def update_report_status(report_id, status, file_path=None):
    """Atualiza o status do relatório no banco de dados"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        if file_path:
            cur.execute("""
                UPDATE reports 
                SET status = %s, file_path = %s, updated_at = %s
                WHERE id = %s
            """, (status, file_path, datetime.now(), report_id))
        else:
            cur.execute("""
                UPDATE reports 
                SET status = %s, updated_at = %s
                WHERE id = %s
            """, (status, datetime.now(), report_id))

        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Erro ao atualizar status do relatório: {e}")
        return False


def poll_flow_status(report_id, flow_run_id, max_attempts=60, interval=5):
    """
    Verifica periodicamente o status do flow run e atualiza no banco

    Args:
        report_id: ID do relatório no banco
        flow_run_id: ID do flow run no Prefect
        max_attempts: Número máximo de tentativas (padrão: 60 = 5 minutos)
        interval: Intervalo entre verificações em segundos (padrão: 5s)
    """
    attempts = 0
    final_states = ['COMPLETED', 'FAILED', 'CANCELLED', 'CRASHED']

    while attempts < max_attempts:
        try:
            status_result = check_flow_run_status(flow_run_id)

            if status_result['success']:
                current_status = status_result['status']

                # Mapeia status do Prefect para status do banco
                status_mapping = {
                    'SCHEDULED': 'scheduled',
                    'PENDING': 'pending',
                    'RUNNING': 'running',
                    'COMPLETED': 'completed',
                    'FAILED': 'failed',
                    'CANCELLED': 'cancelled',
                    'CRASHED': 'failed'
                }

                db_status = status_mapping.get(current_status, 'pending')

                # Atualiza o status no banco
                update_report_status(report_id, db_status)

                print(f"[Polling] Report {report_id} - Status: {current_status}")

                # Se chegou em um estado final, para o polling
                if current_status in final_states:
                    print(f"[Polling] Report {report_id} - Estado final alcançado: {current_status}")
                    break

            time.sleep(interval)
            attempts += 1

        except Exception as e:
            print(f"[Polling] Erro ao verificar status: {e}")
            time.sleep(interval)
            attempts += 1

    if attempts >= max_attempts:
        print(f"[Polling] Report {report_id} - Timeout alcançado após {max_attempts * interval} segundos")
        update_report_status(report_id, 'timeout')


def start_polling_thread(report_id, flow_run_id):
    """Inicia uma thread para fazer polling do status do flow"""
    thread = Thread(target=poll_flow_status, args=(report_id, flow_run_id), daemon=True)
    thread.start()
    return thread


def authenticate(email, password):
    conn = get_db_connection()
    cur = conn.cursor()
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    cur.execute("SELECT id, name, email, role FROM users WHERE email = %s AND password_hash = %s",
                (email, password_hash))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user


if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user' not in st.session_state:
    st.session_state.user = None

try:
    init_db()
except Exception as e:
    st.error(f"Erro de conexão com o banco de dados: {e}")
    st.info("Por favor, certifique-se de que o PostgreSQL está em execução e as credenciais estão corretas")

st.set_page_config(page_title="Portal de Relatórios", page_icon="📊", layout="wide")


def login_page():
    st.title("🔐 Login do Portal de Relatórios")

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("### Bem-vindo")
        email = st.text_input("Email", placeholder="admin@company.com")
        password = st.text_input("Senha", type="password", placeholder="admin123")

        if st.button("Entrar", use_container_width=True):
            try:
                user = authenticate(email, password)
                if user:
                    st.session_state.logged_in = True
                    st.session_state.user = {
                        'id': user[0],
                        'name': user[1],
                        'email': user[2],
                        'role': user[3]
                    }
                    log_audit(user[0], 'login')
                    st.rerun()
                else:
                    st.error("Credenciais inválidas")
            except Exception as e:
                st.error(f"Erro no login: {e}")

        st.info("Credenciais padrão: admin@company.com / admin123")


def main_app():
    st.sidebar.title(f"👤 {st.session_state.user['name']}")
    st.sidebar.write(f"Função: **{st.session_state.user['role'].upper()}**")

    if st.sidebar.button("Sair"):
        log_audit(st.session_state.user['id'], 'logout')
        st.session_state.logged_in = False
        st.session_state.user = None
        st.rerun()

    st.sidebar.markdown("---")

    menu = st.sidebar.radio("Navegação",
                            ["📊 Dashboard", "📄 Relatórios", "🏢 Empresas", "👥 Usuários", "📋 Logs de Auditoria"])

    if menu == "📊 Dashboard":
        dashboard_page()
    elif menu == "📄 Relatórios":
        reports_page()
    elif menu == "🏢 Empresas":
        companies_page()
    elif menu == "👥 Usuários":
        users_page()
    elif menu == "📋 Logs de Auditoria":
        audit_logs_page()


def dashboard_page():
    st.title("📊 Dashboard")

    try:
        conn = get_db_connection()

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_reports = pd.read_sql_query("SELECT COUNT(*) as count FROM reports", conn)['count'][0]
            st.metric("Total de Relatórios", total_reports)

        with col2:
            total_companies = pd.read_sql_query("SELECT COUNT(*) as count FROM company", conn)['count'][0]
            st.metric("Total de Empresas", total_companies)

        with col3:
            total_users = pd.read_sql_query("SELECT COUNT(*) as count FROM users", conn)['count'][0]
            st.metric("Total de Usuários", total_users)

        with col4:
            pending_reports = \
                pd.read_sql_query(
                    "SELECT COUNT(*) as count FROM reports WHERE status IN ('pending', 'scheduled', 'running')", conn)[
                    'count'][0]
            st.metric("Relatórios em Andamento", pending_reports)

        st.markdown("---")

        st.subheader("Relatórios Recentes")
        recent_reports = pd.read_sql_query("""
            SELECT r.id, c.name as empresa, u.name as usuario, r.start_date as data_inicio, 
                   r.end_date as data_fim, r.status, r.generated_at as gerado_em
            FROM reports r
            JOIN company c ON r.company_id = c.id
            JOIN users u ON r.user_id = u.id
            ORDER BY r.created_at DESC
            LIMIT 10
        """, conn)

        if not recent_reports.empty:
            st.dataframe(recent_reports, use_container_width=True)
        else:
            st.info("Nenhum relatório disponível")

        conn.close()
    except Exception as e:
        st.error(f"Erro ao carregar dashboard: {e}")


def reports_page():
    st.title("📄 Gerenciamento de Relatórios")

    tab1, tab2 = st.tabs(["Visualizar Relatórios", "Gerar Novo Relatório"])

    with tab1:
        try:
            conn = get_db_connection()

            col1, col2, col3 = st.columns(3)

            with col1:
                companies = pd.read_sql_query("SELECT id, name FROM company", conn)
                company_filter = st.selectbox("Filtrar por Empresa",
                                              ["Todos"] + companies['name'].tolist())

            with col2:
                status_filter = st.selectbox("Filtrar por Status",
                                             ["Todos", "pending", "scheduled", "running", "completed", "failed",
                                              "timeout"])

            with col3:
                if st.button("🔄 Atualizar", use_container_width=True):
                    st.rerun()

            query = """
                SELECT r.id, c.name as empresa, u.name as usuario, r.start_date as data_inicio, 
                       r.end_date as data_fim, r.status, r.flow_run_id, r.generated_at as gerado_em, 
                       r.file_path as caminho_arquivo
                FROM reports r
                JOIN company c ON r.company_id = c.id
                JOIN users u ON r.user_id = u.id
                WHERE 1=1
            """

            params = []
            if company_filter != "Todos":
                query += " AND c.name = %s"
                params.append(company_filter)

            if status_filter != "Todos":
                query += " AND r.status = %s"
                params.append(status_filter)

            query += " ORDER BY r.created_at DESC"

            if params:
                reports_df = pd.read_sql_query(query, conn, params=params)
            else:
                reports_df = pd.read_sql_query(query, conn)

            if not reports_df.empty:
                # Adiciona indicadores visuais de status
                def format_status(status):
                    status_icons = {
                        'pending': '⏳',
                        'scheduled': '📅',
                        'running': '⚙️',
                        'completed': '✅',
                        'failed': '❌',
                        'timeout': '⏰'
                    }
                    return f"{status_icons.get(status, '❓')} {status}"

                reports_df['status'] = reports_df['status'].apply(format_status)

                st.dataframe(reports_df, use_container_width=True)

                st.subheader("Ações de Relatório")
                report_id = st.number_input("Digite o ID do Relatório", min_value=1, step=1)

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    if st.button("Ver Detalhes"):
                        report = pd.read_sql_query(f"""
                            SELECT r.*, c.name as nome_empresa, u.name as nome_usuario
                            FROM reports r
                            JOIN company c ON r.company_id = c.id
                            JOIN users u ON r.user_id = u.id
                            WHERE r.id = %s
                        """, conn, params=(report_id,))

                        if not report.empty:
                            st.json(report.to_dict('records')[0])
                        else:
                            st.error("Relatório não encontrado")

                with col2:
                    if st.button("Verificar Status Agora"):
                        report = pd.read_sql_query(
                            "SELECT flow_run_id FROM reports WHERE id = %s",
                            conn, params=(report_id,)
                        )

                        if not report.empty and report['flow_run_id'].values[0]:
                            flow_run_id = report['flow_run_id'].values[0]

                            with st.spinner('Verificando status...'):
                                status = check_flow_run_status(flow_run_id)

                            if status['success']:
                                st.success(f"Status atual: {status['status']}")

                                # Atualiza no banco
                                status_mapping = {
                                    'SCHEDULED': 'scheduled',
                                    'PENDING': 'pending',
                                    'RUNNING': 'running',
                                    'COMPLETED': 'completed',
                                    'FAILED': 'failed',
                                    'CANCELLED': 'cancelled',
                                    'CRASHED': 'failed'
                                }
                                db_status = status_mapping.get(status['status'], 'pending')
                                update_report_status(report_id, db_status)

                                st.json(status)
                                st.rerun()
                            else:
                                st.error(f"Erro ao verificar status: {status.get('error')}")
                        else:
                            st.error("Flow Run ID não encontrado para este relatório")

                with col3:
                    if st.button("📥 Baixar Relatório"):
                        report = pd.read_sql_query(
                            "SELECT status, file_path FROM reports WHERE id = %s",
                            conn, params=(report_id,)
                        )

                        if not report.empty:
                            status = report['status'].values[0]
                            file_path = report['file_path'].values[0]

                            if status == 'completed' and file_path:
                                with st.spinner('Baixando relatório do S3...'):
                                    # Verifica se o arquivo existe
                                    if check_file_exists_in_s3(file_path):
                                        file_content = download_report_from_s3(file_path)

                                        if file_content:
                                            # Extrai o nome do arquivo
                                            file_name = file_path.split('/')[-1] if '/' in file_path else file_path

                                            # Cria o botão de download
                                            st.download_button(
                                                label="💾 Clique aqui para baixar",
                                                data=file_content,
                                                file_name=file_name,
                                                mime="application/pdf",
                                                use_container_width=True
                                            )

                                            # Registra a ação no log de auditoria
                                            log_audit(
                                                st.session_state.user['id'],
                                                'download_report',
                                                report_id,
                                                {'file_path': file_path}
                                            )

                                            st.success(f"✅ Relatório pronto para download!")
                                        else:
                                            st.error("❌ Erro ao baixar o arquivo do S3")
                                    else:
                                        st.error("❌ Arquivo não encontrado no S3")
                            elif status != 'completed':
                                st.warning(f"⚠️ Relatório ainda não está completo. Status atual: {status}")
                            else:
                                st.error("❌ Caminho do arquivo não encontrado")
                        else:
                            st.error("Relatório não encontrado")

                with col4:
                    if st.button("Excluir"):
                        cur = conn.cursor()
                        cur.execute("DELETE FROM reports WHERE id = %s", (report_id,))
                        conn.commit()
                        cur.close()
                        log_audit(st.session_state.user['id'], 'delete_report', report_id)
                        st.success("Relatório excluído!")
                        st.rerun()
            else:
                st.info("Nenhum relatório encontrado")

            conn.close()
        except Exception as e:
            st.error(f"Erro ao carregar relatórios: {e}")

    with tab2:
        st.subheader("Gerar Novo Relatório")

        try:
            conn = get_db_connection()
            companies = pd.read_sql_query("SELECT id, name FROM company", conn)

            if companies.empty:
                st.warning("Por favor, adicione empresas primeiro!")
            else:

                company_id = st.selectbox("Selecione a Empresa",
                                          companies['id'].tolist(),
                                          format_func=lambda x: companies[companies['id'] == x]['name'].values[0])

                col1, col2 = st.columns(2)

                with col1:
                    start_date = st.date_input("Data Início", date.today())

                with col2:
                    end_date = st.date_input("Data Fim", date.today())

                with st.expander("Opções Avançadas"):

                    st.markdown("---")
                    st.markdown("**Configurações de Polling:**")
                    enable_polling = st.checkbox("Ativar verificação automática de status", value=True)
                    polling_interval = st.slider("Intervalo de verificação (segundos)", 5, 60, 10)
                    polling_max_time = st.slider("Tempo máximo de verificação (minutos)", 1, 30, 10)

                if st.button("Gerar Relatório", type="primary"):
                    cur = conn.cursor()
                    company = companies[companies['id'] == company_id]['name'].values[0]

                    report_name = f'{company.lower()}_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                    cur.execute("""
                        INSERT INTO reports (company_id, user_id, start_date, end_date, status, file_path, generated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (company_id, st.session_state.user['id'], start_date, end_date,
                          'pending', report_name, datetime.now()))

                    report_id = cur.fetchone()[0]
                    conn.commit()
                    cur.close()

                    flow_parameters = {
                        'company': company,
                        'start_date': str(start_date),
                        'end_date': str(end_date),
                        'report_name': f'{company.lower()}_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                    }

                    with st.spinner('Acionando o flow de geração de relatório...'):
                        result = trigger_prefect_flow(flow_parameters)

                    if result['success']:
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE reports 
                            SET flow_run_id = %s, status = %s
                            WHERE id = %s
                        """, (result['flow_run_id'], 'scheduled', report_id))
                        conn.commit()
                        cur.close()

                        log_audit(st.session_state.user['id'], 'generate_report', report_id,
                                  {**flow_parameters, 'flow_run_id': result['flow_run_id']})

                        st.success(f"✅ Geração de relatório acionada com sucesso!")
                        st.info(f"ID do Relatório: {report_id}")
                        st.info(f"ID da Execução do Flow: {result['flow_run_id']}")

                        # Inicia o polling em background se habilitado
                        if enable_polling:
                            max_attempts = (polling_max_time * 60) // polling_interval
                            thread = start_polling_thread(report_id, result['flow_run_id'])
                            st.success(f"🔄 Verificação automática de status iniciada (a cada {polling_interval}s)")
                            st.info(
                                "O status será atualizado automaticamente no banco de dados. Você pode atualizar a página para ver as mudanças.")

                        # Botão para ir para a aba de visualização
                        if st.button("Ver todos os relatórios"):
                            st.rerun()

                    else:
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE reports 
                            SET status = %s 
                            WHERE id = %s
                        """, ('failed', report_id))
                        conn.commit()
                        cur.close()

                        st.error(f"❌ {result['message']}")
                        st.error(f"Erro: {result.get('error', 'Erro desconhecido')}")

            conn.close()
        except Exception as e:
            st.error(f"Erro ao gerar relatório: {e}")


def companies_page():
    st.title("🏢 Gerenciamento de Empresas")

    if st.session_state.user['role'] != 'admin':
        st.warning("Apenas administradores podem gerenciar empresas")
        return

    tab1, tab2 = st.tabs(["Visualizar Empresas", "Adicionar Empresa"])

    with tab1:
        try:
            conn = get_db_connection()
            companies = pd.read_sql_query("SELECT * FROM company ORDER BY created_at DESC", conn)

            if not companies.empty:
                st.dataframe(companies, use_container_width=True)
            else:
                st.info("Nenhuma empresa encontrada")

            conn.close()
        except Exception as e:
            st.error(f"Erro ao carregar empresas: {e}")

    with tab2:
        st.subheader("Adicionar Nova Empresa")

        name = st.text_input("Nome da Empresa")
        address = st.text_area("Endereço")

        if st.button("Adicionar Empresa"):
            if name:
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("INSERT INTO company (name, address) VALUES (%s, %s) RETURNING id",
                                (name, address))
                    company_id = cur.fetchone()[0]
                    conn.commit()
                    cur.close()

                    log_audit(st.session_state.user['id'], 'add_company', company_id,
                              {'name': name, 'address': address})

                    conn.close()
                    st.success("Empresa adicionada com sucesso!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao adicionar empresa: {e}")
            else:
                st.error("Nome da empresa é obrigatório")


def users_page():
    st.title("👥 Gerenciamento de Usuários")

    if st.session_state.user['role'] != 'admin':
        st.warning("Apenas administradores podem gerenciar usuários")
        return

    tab1, tab2 = st.tabs(["Visualizar Usuários", "Adicionar Usuário"])

    with tab1:
        try:
            conn = get_db_connection()
            users = pd.read_sql_query(
                "SELECT id, name as nome, email, role as funcao, created_at as criado_em FROM users ORDER BY created_at DESC",
                conn)

            if not users.empty:
                st.dataframe(users, use_container_width=True)
            else:
                st.info("Nenhum usuário encontrado")

            conn.close()
        except Exception as e:
            st.error(f"Erro ao carregar usuários: {e}")

    with tab2:
        st.subheader("Adicionar Novo Usuário")

        name = st.text_input("Nome")
        email = st.text_input("Email")
        password = st.text_input("Senha", type="password")
        role = st.selectbox("Função", ["admin", "user", "viewer"])

        if st.button("Adicionar Usuário"):
            if name and email and password:
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    password_hash = hashlib.sha256(password.encode()).hexdigest()

                    cur.execute(
                        "INSERT INTO users (name, email, password_hash, role) VALUES (%s, %s, %s, %s) RETURNING id",
                        (name, email, password_hash, role))
                    user_id = cur.fetchone()[0]
                    conn.commit()
                    cur.close()

                    log_audit(st.session_state.user['id'], 'add_user', user_id,
                              {'name': name, 'email': email, 'role': role})

                    conn.close()
                    st.success("Usuário adicionado com sucesso!")
                    st.rerun()
                except psycopg2.IntegrityError:
                    st.error("Email já existe")
                except Exception as e:
                    st.error(f"Erro ao adicionar usuário: {e}")
            else:
                st.error("Todos os campos são obrigatórios")


def audit_logs_page():
    st.title("📋 Logs de Auditoria")

    if st.session_state.user['role'] != 'admin':
        st.warning("Apenas administradores podem visualizar logs de auditoria")
        return

    try:
        conn = get_db_connection()

        col1, col2 = st.columns(2)

        with col1:
            users = pd.read_sql_query("SELECT id, name FROM users", conn)
            user_filter = st.selectbox("Filtrar por Usuário",
                                       ["Todos"] + users['name'].tolist())

        with col2:
            action_filter = st.selectbox("Filtrar por Ação",
                                         ["Todos", "login", "logout", "generate_report",
                                          "add_company", "add_user", "delete_report"])

        query = """
            SELECT a.id, u.name as usuario, a.action as acao, a.target_id, a.details as detalhes, 
                   a.created_at as criado_em
            FROM audit_logs a
            JOIN users u ON a.user_id = u.id
            WHERE 1=1
        """

        params = []
        if user_filter != "Todos":
            query += " AND u.name = %s"
            params.append(user_filter)

        if action_filter != "Todos":
            query += " AND a.action = %s"
            params.append(action_filter)

        query += " ORDER BY a.created_at DESC LIMIT 100"

        if params:
            logs = pd.read_sql_query(query, conn, params=params)
        else:
            logs = pd.read_sql_query(query, conn)

        if not logs.empty:
            st.dataframe(logs, use_container_width=True)
        else:
            st.info("Nenhum log de auditoria encontrado")

        conn.close()
    except Exception as e:
        st.error(f"Erro ao carregar logs de auditoria: {e}")


if not st.session_state.logged_in:
    login_page()
else:
    main_app()
