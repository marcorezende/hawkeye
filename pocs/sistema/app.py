import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import time

st.set_page_config(
    page_title="Hello World Streamlit",
    page_icon="👋",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .big-font {
        font-size:50px !important;
        font-weight: bold;
        color: #1f77b4;
    }
    </style>
    """, unsafe_allow_html=True)

with st.sidebar:
    st.image("https://streamlit.io/images/brand/streamlit-mark-color.png", width=100)
    st.title("🎛️ Painel de Controle")

    modo = st.radio(
        "Escolha um modo:",
        ["Demonstração Básica", "Gráficos", "Dados Interativos", "Multimídia"]
    )


st.markdown('<p class="big-font">👋 Hello World!</p>', unsafe_allow_html=True)
st.markdown("---")

if modo == "Demonstração Básica":
    st.header("🎯 Demonstração Básica")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📝 Inputs do Usuário")

        nome = st.text_input("Qual é o seu nome?", placeholder="Digite seu nome aqui...")

        idade = st.slider("Qual é a sua idade?", 0, 100, 25)

        cor_favorita = st.selectbox(
            "Qual é sua cor favorita?",
            ["Azul", "Verde", "Vermelho", "Amarelo", "Roxo"]
        )

        hobbies = st.multiselect(
            "Quais são seus hobbies?",
            ["Programação", "Leitura", "Esportes", "Música", "Viagens", "Culinária"]
        )

        aceita_termos = st.checkbox("Aceito os termos de uso")

        if st.button("🚀 Enviar", type="primary"):
            if nome and aceita_termos:
                st.balloons()
                st.success(f"✅ Dados recebidos com sucesso, {nome}!")
            else:
                st.error("❌ Por favor, preencha seu nome e aceite os termos!")

    with col2:
        st.subheader("📊 Resumo dos Dados")

        if nome:
            st.metric("Nome", nome, "✓")
        else:
            st.metric("Nome", "Não informado", "")

        st.metric("Idade", f"{idade} anos", f"{idade - 25:+d}")
        st.metric("Cor Favorita", cor_favorita, "")

        if hobbies:
            st.write("**Seus hobbies:**")
            for hobby in hobbies:
                st.write(f"• {hobby}")
        else:
            st.info("Nenhum hobby selecionado")

        st.markdown("---")
        st.write(f"🕐 **Agora:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

elif modo == "Gráficos":
    st.header("📈 Visualizações de Dados")

    st.subheader("Dados Aleatórios Gerados")

    num_pontos = st.slider("Quantidade de pontos:", 10, 500, 100)

    df = pd.DataFrame({
        'x': range(num_pontos),
        'y': np.random.randn(num_pontos).cumsum(),
        'z': np.random.randn(num_pontos).cumsum()
    })

    tab1, tab2, tab3 = st.tabs(["📊 Linha", "📉 Área", "📍 Scatter"])

    with tab1:
        st.line_chart(df[['y', 'z']])

    with tab2:
        st.area_chart(df['y'])

    with tab3:
        st.scatter_chart(df, x='x', y='y', size='z')

    st.markdown("---")
    st.subheader("📊 Estatísticas Descritivas")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Média Y", f"{df['y'].mean():.2f}")
    with col2:
        st.metric("Desvio Padrão Y", f"{df['y'].std():.2f}")
    with col3:
        st.metric("Mínimo Y", f"{df['y'].min():.2f}")
    with col4:
        st.metric("Máximo Y", f"{df['y'].max():.2f}")

elif modo == "Dados Interativos":
    st.header("🗃️ Tabela de Dados Interativa")

    num_linhas = st.number_input("Número de linhas:", 5, 100, 20)

    dados = {
        'ID': range(1, num_linhas + 1),
        'Nome': [f"Usuário {i}" for i in range(1, num_linhas + 1)],
        'Idade': np.random.randint(18, 70, num_linhas),
        'Cidade': np.random.choice(['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Brasília'], num_linhas),
        'Salário': np.random.randint(3000, 15000, num_linhas),
        'Ativo': np.random.choice([True, False], num_linhas)
    }

    df_dados = pd.DataFrame(dados)

    col1, col2 = st.columns(2)

    with col1:
        idade_min = st.slider("Idade mínima:", 18, 70, 18)

    with col2:
        cidades_filtro = st.multiselect(
            "Filtrar por cidade:",
            options=df_dados['Cidade'].unique(),
            default=df_dados['Cidade'].unique()
        )

    df_filtrado = df_dados[
        (df_dados['Idade'] >= idade_min) &
        (df_dados['Cidade'].isin(cidades_filtro))
        ]

    st.dataframe(df_filtrado, use_container_width=True, height=400)

    st.markdown("---")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total de Registros", len(df_filtrado))
    with col2:
        st.metric("Idade Média", f"{df_filtrado['Idade'].mean():.1f} anos")
    with col3:
        st.metric("Salário Médio", f"R$ {df_filtrado['Salário'].mean():.2f}")

    st.markdown("---")
    csv = df_filtrado.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name="dados_filtrados.csv",
        mime="text/csv"
    )

else:
    st.header("🎬 Multimídia e Elementos Avançados")

    tab1, tab2, tab3, tab4 = st.tabs(["🎨 Cores", "⏱️ Progress", "💬 Mensagens", "🎯 Containers"])

    with tab1:
        st.subheader("Seletor de Cores")
        cor = st.color_picker("Escolha uma cor:", "#1f77b4")
        st.markdown(
            f'<div style="background-color:{cor}; padding:50px; border-radius:10px; text-align:center; color:white; font-size:24px;">Sua cor escolhida!</div>',
            unsafe_allow_html=True)

    with tab2:
        st.subheader("Barra de Progresso")
        if st.button("▶️ Iniciar Simulação"):
            progress_bar = st.progress(0)
            status_text = st.empty()

            for i in range(101):
                progress_bar.progress(i)
                status_text.text(f"Processando... {i}%")
                time.sleep(0.02)

            status_text.text("✅ Concluído!")
            st.success("Processo finalizado com sucesso!")
            st.balloons()

    with tab3:
        st.subheader("Tipos de Mensagens")

        col1, col2 = st.columns(2)

        with col1:
            st.info("ℹ️ Esta é uma mensagem informativa")
            st.success("✅ Esta é uma mensagem de sucesso")

        with col2:
            st.warning("⚠️ Esta é uma mensagem de aviso")
            st.error("❌ Esta é uma mensagem de erro")

        st.markdown("---")

        with st.expander("📖 Ver mais detalhes"):
            st.write("Você pode expandir e colapsar este conteúdo!")
            st.code("""
def hello_world():
    print("Hello, Streamlit!")
            """, language="python")

    with tab4:
        st.subheader("Containers e Layouts")

        with st.container():
            st.write("Este é um container")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.button("Botão 1")
            with col2:
                st.button("Botão 2")
            with col3:
                st.button("Botão 3")

        st.markdown("---")

        with st.container(border=True):
            st.write("🎁 Container com borda")
            st.write("Perfeito para destacar conteúdo importante!")

st.markdown("---")
st.caption("💻 Desenvolvido com Streamlit | 📚 Projeto de Extensão | " + datetime.now().strftime("%Y"))