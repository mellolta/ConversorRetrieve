import streamlit as st
import os
import tempfile
import uuid  # Biblioteca para gerar IDs únicos
from Conversor import processar_dados_pcd, ExportaTabelaMDB

st.set_page_config(page_title="Conversor Retrieve Multiarquivos", layout="wide")

st.title("📂 Conversor de Dados PCD ➔ MDB")

# --- 1. Barra Lateral: Download do Modelo ---
with st.sidebar:
    st.header("Modelos")
    if os.path.exists("template_vazio.mdb"):
        with open("template_vazio.mdb", "rb") as f:
            st.download_button(label="📥 Baixar MDB Vazio", data=f, 
                             file_name="Banco_Vazio.mdb", mime="application/x-msaccess")

# --- 2. Seleção do Banco de Destino (Upload) ---
st.subheader("1. Selecione o Banco MDB de destino")
# O usuário sobe o banco que ele já tem
mdb_user = st.file_uploader("Arraste seu banco .mdb aqui", type=['mdb'])

# --- 3. Upload de Múltiplos CSVs ---
st.subheader("2. Carregue os arquivos CSV da PCD")
uploaded_files = st.file_uploader("Selecione um ou mais arquivos CSV", 
                                  type=['csv'], accept_multiple_files=True)

if mdb_user and uploaded_files:
    if st.button("🚀 Iniciar Conversão e Atualizar Banco"):
        try:
            # Cria um diretório temporário que o SO limpa depois
            with tempfile.TemporaryDirectory() as tmpdirname:
                # Geramos um nome único para evitar conflitos de usuários
                nome_unico = f"banco_{uuid.uuid4().hex}.mdb"
                path_mdb_absoluto = os.path.join(tmpdirname, nome_unico)
                
                # 1. Salva o upload no arquivo temporário
                with open(path_mdb_absoluto, "wb") as f:
                    f.write(mdb_user.getbuffer())
                        
                sucessos = 0
                log_erros = []
                progresso = st.progress(0)
                
                for idx, file in enumerate(uploaded_files):
                    try:
                        # CORREÇÃO DO ERRO: Resetar o ponteiro do arquivo
                        file.seek(0) 
                        
                        # A. Processar os dados
                        df_chuva, df_cota, cod_estacao = processar_dados_pcd(file, file.name)
                        
                        # B. Gravar no MDB
                        tables = {'Chuvas24': df_chuva, 'Cotas24': df_cota}
                        relacaoID = {}
                        for table_name in ['Chuvas24', 'Cotas24']:
                            table_data_raw = tables[table_name].values.tolist()
                            exportador = ExportaTabelaMDB(path_mdb_absoluto, table_name, 
                                                        table_data_raw, [cod_estacao], relacaoID)
                            relacaoID = exportador.exporta_dados_MDB()
                        
                        sucessos += 1
                    except Exception as e:
                        log_erros.append(f"Erro em {file.name}: {e}")
                    
                    progresso.progress((idx + 1) / len(uploaded_files))

                st.balloons()
                if sucessos > 1:
                    st.success(f"✅ Pronto! {sucessos} arquivos processados com sucesso.")
                else:
                    st.success(f"✅ Pronto! {sucessos} arquivo processado com sucesso.")
                
                # 3. Disponibiliza para Download
                # Lemos o conteúdo para a memória antes de oferecer o download
                with open(path_mdb_absoluto, "rb") as f:
                    data_download = f.read()
                    
                st.download_button(
                    label="💾 Baixar Banco Atualizado",
                    data=data_download,
                    file_name=f"Atualizado_{mdb_user.name}",
                    mime="application/x-msaccess"
                )
                
            if log_erros:
                with st.expander("Ver detalhes de erros"):
                    for erro in log_erros: st.warning(erro)

        except Exception as e:
            st.error(f"Erro crítico: {e}")

# --- 4. Prévia (com correção do ponteiro) ---
if uploaded_files:
    st.divider()
    if st.checkbox("Mostrar prévia do último CSV carregado"):
        last_file = uploaded_files[-1]
        last_file.seek(0) # CORREÇÃO AQUI TAMBÉM
        df_p, _, _ = processar_dados_pcd(last_file, last_file.name)
        st.write(f"Prévia: {last_file.name}")
        st.dataframe(df_p.head(5))