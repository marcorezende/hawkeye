import json
import os
import time
from datetime import datetime, date, timedelta
from threading import Thread

import bcrypt
import boto3
import fitz  # PyMuPDF para preview de PDF
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import requests
import streamlit as st
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# CONFIGURAÇÕES DE SEGURANÇA
# ============================================================================

# Rate Limiting para Login
if 'LOGIN_ATTEMPTS' not in st.session_state:
    st.session_state['LOGIN_ATTEMPTS'] = {}
LOGIN_ATTEMPTS = st.session_state['LOGIN_ATTEMPTS']
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION = 300  # 5 minutos em segundos

# Configuração do Banco de Dados
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB_PORTAL', 'portal'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

# Validação de credenciais obrigatórias
if not all([DB_CONFIG['user'], DB_CONFIG['password']]):
    raise ValueError("POSTGRES_USER e POSTGRES_PASSWORD devem estar definidos nas variáveis de ambiente")

# Configuração do Prefect
PREFECT_API_URL = os.getenv('PREFECT_API_URL')
PREFECT_USERNAME = os.getenv('PREFECT_USERNAME')
PREFECT_PASSWORD = os.getenv('PREFECT_PASSWORD')

if not all([PREFECT_API_URL, PREFECT_USERNAME, PREFECT_PASSWORD]):
    raise ValueError("Variáveis de ambiente do Prefect não configuradas corretamente")

# Configuração do S3/MinIO
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
S3_BUCKET = os.getenv('MINIO_BUCKET')
S3_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_ACCESS_KEY_ID = os.getenv('MINIO_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('MINIO_SECRET_KEY')

if not all([MINIO_ENDPOINT, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
    raise ValueError("Variáveis de ambiente do MinIO/S3 não configuradas corretamente")

# Configurações de paginação
ITEMS_PER_PAGE = 10


# ============================================================================
# FUNÇÕES DE SEGURANÇA
# ============================================================================

def hash_password(password: str) -> bytes:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())


def verify_password(password: str, hashed) -> bool:
    try:
        if isinstance(hashed, memoryview):
            hashed_bytes = hashed.tobytes()
        elif isinstance(hashed, str):
            hashed_bytes = hashed.encode('utf-8')
        else:
            hashed_bytes = hashed

        encoded_password = password.encode('utf-8')

        return bcrypt.checkpw(encoded_password, hashed_bytes)
    except Exception as e:
        return False


def check_rate_limit(identifier: str) -> tuple[bool, int]:
    current_time = time.time()

    if identifier not in LOGIN_ATTEMPTS:
        return True, 0

    attempt_data = LOGIN_ATTEMPTS[identifier]

    if current_time - attempt_data['timestamp'] > LOCKOUT_DURATION:
        del LOGIN_ATTEMPTS[identifier]
        return True, 0

    if attempt_data['count'] >= MAX_LOGIN_ATTEMPTS:
        time_remaining = int(LOCKOUT_DURATION - (current_time - attempt_data['timestamp']))
        return False, time_remaining

    return True, 0


def record_login_attempt(identifier: str, success: bool):
    current_time = time.time()
    if success:
        if identifier in LOGIN_ATTEMPTS:
            del LOGIN_ATTEMPTS[identifier]
    else:
        if identifier not in LOGIN_ATTEMPTS:
            LOGIN_ATTEMPTS[identifier] = {'count': 1, 'timestamp': current_time}
        else:
            LOGIN_ATTEMPTS[identifier]['count'] += 1
            LOGIN_ATTEMPTS[identifier]['timestamp'] = current_time

        # Debug: mostra o estado atual
        print(f"[Rate Limit] {identifier}: {LOGIN_ATTEMPTS[identifier]['count']} tentativas")


def sanitize_input(value: str, max_length: int = 255) -> str:
    if not value:
        return ""
    sanitized = value.strip()[:max_length]
    return sanitized


def validate_email(email: str) -> bool:
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


# ============================================================================
# FUNÇÕES DO BANCO DE DADOS (COM PROTEÇÃO SQL INJECTION)
# ============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash BYTEA NOT NULL,
        role VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS company (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        address TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS reports (
        id SERIAL PRIMARY KEY,
        company_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        file_path VARCHAR(500),
        status VARCHAR(50) NOT NULL,
        flow_run_id VARCHAR(255),
        generated_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (company_id) REFERENCES company(id) ON DELETE CASCADE
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS audit_logs (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        action VARCHAR(100) NOT NULL,
        target_id INTEGER,
        details JSONB,
        ip_address VARCHAR(45),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )''')

    # Índices para performance
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_reports_user_id ON reports(user_id)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_reports_company_id ON reports(company_id)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports(created_at)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id)''')
    cur.execute('''CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at)''')

    # Migração: Adiciona coluna flow_run_id se não existir
    try:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name=%s AND column_name=%s
        """, ('reports', 'flow_run_id'))
        if not cur.fetchone():
            cur.execute("ALTER TABLE reports ADD COLUMN flow_run_id VARCHAR(255)")
            conn.commit()
    except Exception as e:
        print(f"Aviso na migração: {e}")
        conn.rollback()

    # Criar usuário admin inicial apenas se não existir
    cur.execute("SELECT * FROM users WHERE email = %s", ('admin@company.com',))
    if not cur.fetchone():
        admin_password = os.getenv('ADMIN_INITIAL_PASSWORD', 'Admin@123!Change')
        password_hash = hash_password(admin_password)
        cur.execute(
            "INSERT INTO users (name, email, password_hash, role) VALUES (%s, %s, %s, %s)",
            ('Admin User', 'admin@company.com', password_hash, 'admin')
        )
        print("⚠️  Usuário admin criado. Senha inicial:", admin_password)
        print("⚠️  ALTERE A SENHA IMEDIATAMENTE!")

    # Empresas de exemplo
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

    for company in companies:
        cur.execute("""
            INSERT INTO company (name, address)
            VALUES (%s, NULL)
            ON CONFLICT (name) DO NOTHING
        """, (company,))

    conn.commit()
    cur.close()
    conn.close()


def log_audit(user_id: int, action: str, target_id: int = None, details: dict = None, ip_address: str = None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO audit_logs (user_id, action, target_id, details, ip_address) VALUES (%s, %s, %s, %s, %s)",
            (user_id, action, target_id, json.dumps(details) if details else None, ip_address)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao registrar auditoria: {e}")


def authenticate(email: str, password: str) -> tuple:
    if not validate_email(email):
        return None

    # Verifica rate limit
    allowed, time_remaining = check_rate_limit(email)
    if not allowed:
        raise Exception(f"Muitas tentativas de login. Tente novamente em {time_remaining} segundos")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, email, password_hash, role FROM users WHERE email = %s",
            (email,)
        )
        user = cur.fetchone()
        cur.close()
        conn.close()
        if user and verify_password(password, user[3]):
            record_login_attempt(email, True)
            # Retorna tupla sem o hash da senha
            return (user[0], user[1], user[2], user[4])
        else:
            record_login_attempt(email, False)
            print(LOGIN_ATTEMPTS)
            return None

    except Exception as e:
        print(f"Erro na autenticação: {e}")
        return None


# ============================================================================
# FUNÇÕES S3/MinIO
# ============================================================================

def get_s3_client():
    """Cria cliente S3 seguro"""
    return boto3.client(
        's3',
        region_name=S3_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=MINIO_ENDPOINT
    )


def download_report_from_s3(file_path: str) -> bytes:
    """Download seguro de relatório do S3"""
    # Sanitiza o caminho para evitar path traversal
    file_path = file_path.replace('..', '').replace('//', '/')
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
        print(f"Erro inesperado: {e}")
        return None


def check_file_exists_in_s3(file_path: str) -> bool:
    """Verifica se arquivo existe no S3"""
    file_path = file_path.replace('..', '').replace('//', '/')
    full_path = f"lm/reports/{file_path}"

    try:
        s3_client = get_s3_client()
        s3_client.head_object(Bucket=S3_BUCKET, Key=full_path)
        return True
    except ClientError as e:
        return e.response['Error']['Code'] != '404'
    except Exception:
        return False


def generate_pdf_preview(pdf_content: bytes, max_pages: int = 3) -> tuple:
    """Gera preview seguro das primeiras páginas do PDF"""
    try:
        pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
        preview_images = []
        total_pages = len(pdf_document)

        for page_num in range(min(max_pages, total_pages)):
            page = pdf_document[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_data = pix.tobytes("png")
            preview_images.append(img_data)
            pix = None

        pdf_document.close()
        return preview_images, total_pages
    except Exception as e:
        print(f"Erro ao gerar preview: {e}")
        return None, 0


# ============================================================================
# FUNÇÕES PREFECT
# ============================================================================

def trigger_prefect_flow(parameters: dict) -> dict:
    """Aciona flow do Prefect de forma segura"""
    try:
        headers = {'Content-Type': 'application/json'}
        url = f"{PREFECT_API_URL}/flow_runs/"

        payload = {
            'parameters': parameters,
            'flow_id': os.getenv('PREFECT_FLOW_ID'),
            'deployment_id': os.getenv('PREFECT_DEPLOYMENT_ID'),
            'work_pool_name': os.getenv('PREFECT_WORK_POOL', 'lm'),
            'state': {'type': 'SCHEDULED'}
        }

        response = requests.post(
            url,
            json=payload,
            headers=headers,
            auth=(PREFECT_USERNAME, PREFECT_PASSWORD),
            timeout=10
        )
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
            'message': f'Falha ao acionar o flow: {str(e)}'
        }


def check_flow_run_status(flow_run_id: str) -> dict:
    """Verifica status do flow de forma segura"""
    try:
        headers = {'Content-Type': 'application/json'}
        url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}"

        response = requests.get(
            url,
            headers=headers,
            auth=(PREFECT_USERNAME, PREFECT_PASSWORD),
            timeout=10
        )
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
        return {'success': False, 'error': str(e)}


def update_report_status(report_id: int, status: str, file_path: str = None) -> bool:
    """Atualiza status do relatório de forma segura"""
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
        print(f"Erro ao atualizar status: {e}")
        return False


def poll_flow_status(report_id: int, flow_run_id: str, max_attempts: int = 600, interval: int = 5):
    """Polling do status do flow"""
    attempts = 0
    final_states = ['COMPLETED', 'FAILED', 'CANCELLED', 'CRASHED']

    while attempts < max_attempts:
        try:
            status_result = check_flow_run_status(flow_run_id)
            if status_result['success']:
                current_status = status_result['status']
                status_mapping = {
                    'SCHEDULED': 'scheduled', 'PENDING': 'pending',
                    'RUNNING': 'running', 'COMPLETED': 'completed',
                    'FAILED': 'failed', 'CANCELLED': 'cancelled',
                    'CRASHED': 'failed'
                }
                db_status = status_mapping.get(current_status, 'pending')
                update_report_status(report_id, db_status)

                if current_status in final_states:
                    break

            time.sleep(interval)
            attempts += 1
        except Exception as e:
            print(f"[Polling] Erro: {e}")
            time.sleep(interval)
            attempts += 1

    if attempts >= max_attempts:
        update_report_status(report_id, 'timeout')


def start_polling_thread(report_id: int, flow_run_id: str):
    """Inicia thread de polling"""
    thread = Thread(target=poll_flow_status, args=(report_id, flow_run_id), daemon=True)
    thread.start()
    return thread


def get_dashboard_stats(conn):
    """Obtém estatísticas do dashboard de forma segura"""
    stats = {}

    stats['total_reports'] = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM reports", conn
    )['count'][0]

    stats['total_companies'] = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM company", conn
    )['count'][0]

    stats['total_users'] = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM users", conn
    )['count'][0]

    stats['pending_reports'] = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM reports WHERE status IN ('pending', 'scheduled', 'running')",
        conn
    )['count'][0]

    stats['reports_by_status'] = pd.read_sql_query(
        "SELECT status, COUNT(*) as count FROM reports GROUP BY status", conn
    )

    stats['reports_by_company'] = pd.read_sql_query("""
        SELECT c.name, COUNT(r.id) as count
        FROM reports r
        JOIN company c ON r.company_id = c.id
        GROUP BY c.name
        ORDER BY count DESC
        LIMIT 10
    """, conn)

    stats['reports_over_time'] = pd.read_sql_query("""
        SELECT DATE(created_at) as date, COUNT(*) as count
        FROM reports
        WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE(created_at)
        ORDER BY date
    """, conn)

    return stats


# ============================================================================
# INTERFACE STREAMLIT
# ============================================================================

if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user' not in st.session_state:
    st.session_state.user = None
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

try:
    init_db()
except Exception as e:
    print(e)
    st.error("Erro ao inicializar banco de dados")
    st.stop()

st.set_page_config(
    page_title="Portal de Relatórios",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


def login_page():
    """Página de login com rate limiting"""
    st.title("🔐 Login do Portal de Relatórios")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### Bem-vindo")

        email = st.text_input("Email", placeholder="seu@email.com")
        password = st.text_input("Senha", type="password")

        if st.button("Entrar", use_container_width=True):
            if not email or not password:
                st.error("Preencha todos os campos")
                return

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
                    st.success("Login realizado com sucesso!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Credenciais inválidas")
            except Exception as e:
                st.error(f"⚠️ {str(e)}")

        st.info("💡 Entre em contato com o administrador para obter acesso")


def main_app():
    """Aplicação principal"""
    st.sidebar.title(f"👤 {st.session_state.user['name']}")
    st.sidebar.write(f"Função: **{st.session_state.user['role'].upper()}**")

    if st.sidebar.button("🚪 Sair"):
        log_audit(st.session_state.user['id'], 'logout')
        st.session_state.logged_in = False
        st.session_state.user = None
        st.rerun()

    st.sidebar.markdown("---")

    menu = st.sidebar.radio(
        "Navegação",
        ["📊 Dashboard", "📄 Relatórios", "🏢 Empresas", "👥 Usuários", "📋 Logs de Auditoria"]
    )

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
    """Dashboard com gráficos"""
    st.title("📊 Dashboard")

    try:
        conn = get_db_connection()
        stats = get_dashboard_stats(conn)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Relatórios", stats['total_reports'])
        with col2:
            st.metric("Total de Empresas", stats['total_companies'])
        with col3:
            st.metric("Total de Usuários", stats['total_users'])
        with col4:
            st.metric("Em Andamento", stats['pending_reports'])

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 Relatórios por Status")
            if not stats['reports_by_status'].empty:
                fig = px.pie(
                    stats['reports_by_status'],
                    values='count',
                    names='status',
                    color='status',
                    color_discrete_map={
                        'completed': '#28a745',
                        'pending': '#ffc107',
                        'running': '#17a2b8',
                        'failed': '#dc3545',
                        'scheduled': '#6c757d',
                        'timeout': '#fd7e14'
                    },
                    hole=0.4
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Sem dados disponíveis")

        with col2:
            st.subheader("🏢 Top 10 Empresas")
            if not stats['reports_by_company'].empty:
                fig = px.bar(
                    stats['reports_by_company'],
                    x='count',
                    y='name',
                    orientation='h',
                    color='count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(
                    height=400,
                    yaxis={'categoryorder': 'total ascending'},
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Sem dados disponíveis")

        st.subheader("📅 Relatórios - Últimos 30 dias")
        if not stats['reports_over_time'].empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=stats['reports_over_time']['date'],
                y=stats['reports_over_time']['count'],
                mode='lines+markers',
                name='Relatórios',
                line=dict(color='#007bff', width=3),
                marker=dict(size=8)
            ))
            fig.update_layout(
                height=400,
                xaxis_title="Data",
                yaxis_title="Quantidade",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados dos últimos 30 dias")

        conn.close()
    except Exception as e:
        st.error("Erro ao carregar dashboard")
        print(f"Erro: {e}")


def reports_page():
    """Página de gerenciamento de relatórios"""
    st.title("📄 Gerenciamento de Relatórios")

    tab1, tab2 = st.tabs(["Visualizar Relatórios", "Gerar Novo Relatório"])

    with tab1:
        try:
            conn = get_db_connection()

            # Filtros
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                companies = pd.read_sql_query("SELECT id, name FROM company ORDER BY name", conn)
                company_filter = st.selectbox("Empresa", ["Todos"] + companies['name'].tolist())

            with col2:
                status_filter = st.selectbox(
                    "Status",
                    ["Todos", "pending", "scheduled", "running", "completed", "failed", "timeout"]
                )

            with col3:
                date_filter = st.date_input(
                    "Data desde",
                    value=date.today() - timedelta(days=30),
                    max_value=date.today()
                )

            with col4:
                if st.button("🔄 Atualizar", use_container_width=True):
                    st.rerun()

            # Query base com parâmetros seguros
            query = """
                SELECT r.id, c.name as empresa, u.name as usuario,
                       r.start_date as data_inicio, r.end_date as data_fim,
                       r.status, r.created_at as criado_em, r.file_path
                FROM reports r
                JOIN company c ON r.company_id = c.id
                JOIN users u ON r.user_id = u.id
                WHERE r.created_at >= %s
            """
            params = [date_filter]

            if company_filter != "Todos":
                query += " AND c.name = %s"
                params.append(company_filter)

            if status_filter != "Todos":
                query += " AND r.status = %s"
                params.append(status_filter)

            # Count total
            count_query = f"SELECT COUNT(*) as total FROM ({query}) as subquery"
            total_reports = pd.read_sql_query(count_query, conn, params=params)['total'][0]

            # Paginação
            total_pages = max(1, (total_reports + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                current_page = st.number_input(
                    f"Página (1-{total_pages})",
                    min_value=1,
                    max_value=total_pages,
                    value=min(st.session_state.current_page, total_pages),
                    key='page_selector'
                )
                st.session_state.current_page = current_page

            # Query com paginação
            offset = (current_page - 1) * ITEMS_PER_PAGE
            query += f" ORDER BY r.created_at DESC LIMIT %s OFFSET %s"
            params.extend([ITEMS_PER_PAGE, offset])

            reports_df = pd.read_sql_query(query, conn, params=params)

            if not reports_df.empty:
                def format_status(status):
                    icons = {
                        'pending': '⏳', 'scheduled': '📅', 'running': '⚙️',
                        'completed': '✅', 'failed': '❌', 'timeout': '⏰'
                    }
                    return f"{icons.get(status, '❓')} {status}"

                reports_df['status'] = reports_df['status'].apply(format_status)
                st.dataframe(reports_df, use_container_width=True)
                st.info(f"Mostrando {len(reports_df)} de {total_reports} | Página {current_page} de {total_pages}")

                # Ações
                st.subheader("Ações de Relatório")
                report_id = st.number_input("ID do Relatório", min_value=1, step=1)

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    if st.button("Ver Detalhes"):
                        report = pd.read_sql_query("""
                            SELECT r.*, c.name as empresa, u.name as usuario
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
                    if st.button("👁️ Preview"):
                        report = pd.read_sql_query(
                            "SELECT status, file_path FROM reports WHERE id = %s",
                            conn, params=(report_id,)
                        )
                        if not report.empty:
                            status = report['status'].values[0].split()[-1]
                            file_path = report['file_path'].values[0]

                            if status == 'completed' and file_path:
                                if check_file_exists_in_s3(file_path):
                                    with st.spinner('Carregando preview...'):
                                        content = download_report_from_s3(file_path)
                                        if content:
                                            images, total = generate_pdf_preview(content)
                                            if images:
                                                st.success(f"📄 Preview (Total: {total} páginas)")
                                                for idx, img in enumerate(images):
                                                    st.image(img, caption=f"Página {idx + 1}",
                                                             use_container_width=True)
                                            else:
                                                st.error("Erro ao gerar preview")
                                        else:
                                            st.error("Erro ao baixar arquivo")
                                else:
                                    st.error("Arquivo não encontrado")
                            else:
                                st.warning(f"Relatório não completo. Status: {status}")
                        else:
                            st.error("Relatório não encontrado")

                with col3:
                    if st.button("📥 Baixar"):
                        report = pd.read_sql_query(
                            "SELECT status, file_path FROM reports WHERE id = %s",
                            conn, params=(report_id,)
                        )
                        if not report.empty:
                            status = report['status'].values[0].split()[-1]
                            file_path = report['file_path'].values[0]

                            if status == 'completed' and file_path:
                                if check_file_exists_in_s3(file_path):
                                    content = download_report_from_s3(file_path)
                                    if content:
                                        file_name = file_path.split('/')[-1]
                                        st.download_button(
                                            label="💾 Clique para baixar",
                                            data=content,
                                            file_name=file_name,
                                            mime="application/pdf",
                                            use_container_width=True
                                        )
                                        log_audit(
                                            st.session_state.user['id'],
                                            'download_report',
                                            report_id,
                                            {'file_path': file_path}
                                        )
                                        st.success("✅ Pronto!")
                                    else:
                                        st.error("Erro ao baixar")
                                else:
                                    st.error("Arquivo não encontrado")
                            else:
                                st.warning(f"Status: {status}")
                        else:
                            st.error("Relatório não encontrado")

                with col4:
                    if st.button("🗑️ Excluir"):
                        if st.session_state.user['role'] == 'admin':
                            cur = conn.cursor()
                            cur.execute("DELETE FROM reports WHERE id = %s", (report_id,))
                            conn.commit()
                            cur.close()
                            log_audit(st.session_state.user['id'], 'delete_report', report_id)
                            st.success("Relatório excluído!")
                            st.rerun()
                        else:
                            st.error("Apenas administradores")
            else:
                st.info("Nenhum relatório encontrado")

            conn.close()
        except Exception as e:
            st.error("Erro ao carregar relatórios")
            print(f"Erro: {e}")

    with tab2:
        st.subheader("Gerar Novo Relatório")
        try:
            conn = get_db_connection()
            companies = pd.read_sql_query("SELECT id, name FROM company ORDER BY name", conn)

            if companies.empty:
                st.warning("Adicione empresas primeiro!")
            else:
                company_id = st.selectbox(
                    "Empresa",
                    companies['id'].tolist(),
                    format_func=lambda x: companies[companies['id'] == x]['name'].values[0]
                )

                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Data Início", date.today())
                with col2:
                    end_date = st.date_input("Data Fim", date.today())

                if st.button("Gerar Relatório", type="primary"):
                    if end_date < start_date:
                        st.error("Data fim deve ser maior que data início")
                        return

                    cur = conn.cursor()
                    company = companies[companies['id'] == company_id]['name'].values[0]
                    report_name = f'{sanitize_input(company.lower())}_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'

                    cur.execute("""
                        INSERT INTO reports (company_id, user_id, start_date, end_date, status, file_path, generated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (company_id, st.session_state.user['id'], start_date, end_date,
                          'pending', report_name, datetime.now()))

                    report_id = cur.fetchone()[0]
                    conn.commit()
                    cur.close()

                    flow_params = {
                        'company': company,
                        'start_date': str(start_date),
                        'end_date': str(end_date),
                        'report_name': report_name
                    }

                    with st.spinner('Acionando geração...'):
                        result = trigger_prefect_flow(flow_params)

                    if result['success']:
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE reports 
                            SET flow_run_id = %s, status = %s
                            WHERE id = %s
                        """, (result['flow_run_id'], 'scheduled', report_id))
                        conn.commit()
                        cur.close()

                        log_audit(
                            st.session_state.user['id'],
                            'generate_report',
                            report_id,
                            {**flow_params, 'flow_run_id': result['flow_run_id']}
                        )

                        st.success(f"✅ Relatório acionado! ID: {report_id}")
                        start_polling_thread(report_id, result['flow_run_id'])
                    else:
                        cur = conn.cursor()
                        cur.execute("UPDATE reports SET status = %s WHERE id = %s", ('failed', report_id))
                        conn.commit()
                        cur.close()
                        st.error(f"❌ {result['message']}")

            conn.close()
        except Exception as e:
            st.error("Erro ao gerar relatório")
            print(f"Erro: {e}")


def companies_page():
    """Página de gerenciamento de empresas"""
    st.title("🏢 Gerenciamento de Empresas")

    if st.session_state.user['role'] != 'admin':
        st.warning("Apenas administradores podem gerenciar empresas")
        return

    tab1, tab2 = st.tabs(["Visualizar", "Adicionar"])

    with tab1:
        try:
            conn = get_db_connection()
            companies = pd.read_sql_query(
                "SELECT id, name as nome, address as endereco, created_at as criado_em FROM company ORDER BY name",
                conn
            )
            if not companies.empty:
                st.dataframe(companies, use_container_width=True)
            else:
                st.info("Nenhuma empresa encontrada")
            conn.close()
        except Exception as e:
            st.error("Erro ao carregar empresas")
            print(f"Erro: {e}")

    with tab2:
        st.subheader("Adicionar Nova Empresa")
        name = st.text_input("Nome da Empresa", max_chars=255)
        address = st.text_area("Endereço", max_chars=500)

        if st.button("Adicionar"):
            if not name:
                st.error("Nome é obrigatório")
                return

            name = sanitize_input(name, 255)
            address = sanitize_input(address, 500)

            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO company (name, address) VALUES (%s, %s) RETURNING id",
                    (name, address)
                )
                company_id = cur.fetchone()[0]
                conn.commit()
                cur.close()
                conn.close()

                log_audit(
                    st.session_state.user['id'],
                    'add_company',
                    company_id,
                    {'name': name}
                )

                st.success("Empresa adicionada!")
                st.rerun()
            except psycopg2.IntegrityError:
                st.error("Empresa já existe")
            except Exception as e:
                st.error("Erro ao adicionar empresa")
                print(f"Erro: {e}")


def users_page():
    """Página de gerenciamento de usuários"""
    st.title("👥 Gerenciamento de Usuários")

    if st.session_state.user['role'] != 'admin':
        st.warning("Apenas administradores podem gerenciar usuários")
        return

    tab1, tab2 = st.tabs(["Visualizar", "Adicionar"])

    with tab1:
        try:
            conn = get_db_connection()
            users = pd.read_sql_query("""
                SELECT id, name as nome, email, role as funcao, 
                       created_at as criado_em 
                FROM users 
                ORDER BY created_at DESC
            """, conn)
            if not users.empty:
                st.dataframe(users, use_container_width=True)
            else:
                st.info("Nenhum usuário encontrado")
            conn.close()
        except Exception as e:
            st.error("Erro ao carregar usuários")
            print(f"Erro: {e}")

    with tab2:
        st.subheader("Adicionar Novo Usuário")
        name = st.text_input("Nome Completo", max_chars=255)
        email = st.text_input("Email", max_chars=255)
        password = st.text_input("Senha", type="password", max_chars=100)
        password_confirm = st.text_input("Confirmar Senha", type="password", max_chars=100)
        role = st.selectbox("Função", ["admin", "user", "viewer"])

        if st.button("Adicionar Usuário"):
            if not all([name, email, password, password_confirm]):
                st.error("Preencha todos os campos")
                return

            if password != password_confirm:
                st.error("Senhas não coincidem")
                return

            if len(password) < 8:
                st.error("Senha deve ter no mínimo 8 caracteres")
                return

            if not validate_email(email):
                st.error("Email inválido")
                return

            name = sanitize_input(name, 255)
            email = sanitize_input(email, 255)

            try:
                conn = get_db_connection()
                cur = conn.cursor()
                password_hash = hash_password(password)

                cur.execute(
                    "INSERT INTO users (name, email, password_hash, role) VALUES (%s, %s, %s, %s) RETURNING id",
                    (name, email, password_hash, role)
                )
                user_id = cur.fetchone()[0]
                conn.commit()
                cur.close()
                conn.close()

                log_audit(
                    st.session_state.user['id'],
                    'add_user',
                    user_id,
                    {'name': name, 'email': email, 'role': role}
                )

                st.success("Usuário adicionado!")
                st.rerun()
            except psycopg2.IntegrityError:
                st.error("Email já existe")
            except Exception as e:
                st.error("Erro ao adicionar usuário")
                print(f"Erro: {e}")


def audit_logs_page():
    """Página de logs de auditoria"""
    st.title("📋 Logs de Auditoria")

    if st.session_state.user['role'] != 'admin':
        st.warning("Apenas administradores podem visualizar logs")
        return

    try:
        conn = get_db_connection()

        col1, col2 = st.columns(2)

        with col1:
            users = pd.read_sql_query("SELECT id, name FROM users ORDER BY name", conn)
            user_filter = st.selectbox("Usuário", ["Todos"] + users['name'].tolist())

        with col2:
            action_filter = st.selectbox(
                "Ação",
                ["Todos", "login", "logout", "generate_report", "download_report",
                 "add_company", "add_user", "delete_report"]
            )

        query = """
            SELECT a.id, u.name as usuario, a.action as acao,
                   a.target_id, a.details as detalhes,
                   a.ip_address as ip, a.created_at as data_hora
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
            st.info(f"Mostrando últimos {len(logs)} registros")
        else:
            st.info("Nenhum log encontrado")

        conn.close()
    except Exception as e:
        st.error("Erro ao carregar logs")
        print(f"Erro: {e}")


# ============================================================================
# MAIN
# ============================================================================

if not st.session_state.logged_in:
    login_page()
else:
    main_app()
