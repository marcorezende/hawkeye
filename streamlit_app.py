import hashlib
import json
import os
from datetime import datetime, date

import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv

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
        name VARCHAR(255),
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

    cur.execute("SELECT * FROM users WHERE email = 'admin@company.com'")
    if not cur.fetchone():
        password = hashlib.sha256('admin123'.encode()).hexdigest()
        cur.execute("INSERT INTO users (name, email, password_hash, role) VALUES (%s, %s, %s, %s)",
                    ('Admin User', 'admin@company.com', password, 'admin'))

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
                pd.read_sql_query("SELECT COUNT(*) as count FROM reports WHERE status = 'pending'", conn)['count'][0]
            st.metric("Relatórios Pendentes", pending_reports)

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
                                             ["Todos", "pending", "completed", "failed"])

            with col3:
                st.write("")

            query = """
                SELECT r.id, c.name as empresa, u.name as usuario, r.start_date as data_inicio, 
                       r.end_date as data_fim, r.status, r.generated_at as gerado_em, r.file_path as caminho_arquivo
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
                st.dataframe(reports_df, use_container_width=True)

                st.subheader("Ações de Relatório")
                report_id = st.number_input("Digite o ID do Relatório", min_value=1, step=1)

                col1, col2 = st.columns(2)

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
                    if st.button("Excluir Relatório"):
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
                    report_format = st.selectbox("Formato do Relatório", ["PDF", "Excel", "CSV"])
                    include_charts = st.checkbox("Incluir Gráficos", value=True)
                    email_notification = st.checkbox("Enviar Notificação por Email", value=False)

                if st.button("Gerar Relatório", type="primary"):
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO reports (company_id, user_id, start_date, end_date, status, generated_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (company_id, st.session_state.user['id'], start_date, end_date,
                          'pending', datetime.now()))

                    report_id = cur.fetchone()[0]
                    conn.commit()
                    cur.close()

                    flow_parameters = {
                        'report_id': report_id,
                        'company_id': company_id,
                        'user_id': st.session_state.user['id'],
                        'start_date': str(start_date),
                        'end_date': str(end_date),
                        'report_format': report_format,
                        'include_charts': include_charts,
                        'email_notification': email_notification
                    }

                    with st.spinner('Acionando o flow de geração de relatório...'):
                        result = trigger_prefect_flow(flow_parameters)

                    if result['success']:
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE reports 
                            SET file_path = %s 
                            WHERE id = %s
                        """, (f"flow_run:{result['flow_run_id']}", report_id))
                        conn.commit()
                        cur.close()

                        log_audit(st.session_state.user['id'], 'generate_report', report_id,
                                  {**flow_parameters, 'flow_run_id': result['flow_run_id']})

                        st.success(f"✅ Geração de relatório acionada com sucesso!")
                        st.info(f"ID do Relatório: {report_id}")
                        st.info(f"ID da Execução do Flow: {result['flow_run_id']}")

                        if st.button("Verificar Status"):
                            status = check_flow_run_status(result['flow_run_id'])
                            if status['success']:
                                st.json(status)
                            else:
                                st.error(f"Não foi possível verificar o status: {status.get('error')}")
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
