# -*- coding: utf-8 -*-
"""
Conversor Retieve
converte os retrieves das PCDs em um banco .mdb

Versão para o Streamlit.

"""
#<===============================================================================================================================
#$ Bibliotecas

import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import os
import platform
import tempfile

#< ------------------------------------------------------------------------------------------------------------------------------
def processar_dados_pcd(arquivo_csv, nome_arquivo):
    """
    arquivo_csv: pode ser o caminho (str) ou o objeto do Streamlit (UploadedFile)
    nome_arquivo: string com o nome para extrair o código da estação
    """
    #- 2. Carregar o CSV
    #< O CSV tem duas linhas de cabeçalho, vamos pular a primeira (descritiva)
    df_retrieve = pd.read_csv(arquivo_csv, skiprows=1, 
                            na_values=[-99999, -99888])

    #- Selecionar e renomear as colunas desejadas
    df = df_retrieve[['time', 'sid', 'Avg', 'PP_Acum']].copy()
    df.columns = ['DataHora', 'EstacaoCodigo', 'Cota', 'PrecAc']

    #- Converter a coluna de tempo para o formato datetime
    #< %m = mês, %d = dia, %y = ano (2 dígitos), %I = hora (12h), %M = min, %S = seg, %p = AM/PM
    df['DataHora'] = pd.to_datetime(df['DataHora'], format='%m/%d/%y %I:%M:%S %p')

    #- obtém o código do nome do arquivo
    codigo_nome_arquivo = int(nome_arquivo[5:13])

    #- Obtém os códigos únicos presentes no DF, ignorando os NAs (dropna=True)
    codigos_no_df = df['EstacaoCodigo'].dropna().unique()

    #- vadidação do código
    if len(codigos_no_df) > 0 and codigos_no_df[0] != codigo_nome_arquivo:
        # Se o código existe mas é diferente do nome do arquivo, paramos aqui.
        raise ValueError(f"ERRO: Código no CSV ({codigos_no_df[0]}) difere do nome do retrieve ({codigo_nome_arquivo})")

    #< ------------------------------------------------------------------------------------------------------------------------------
    #- Calcula a precipitação de 15 min (diferença entre a linha atual e a anterior)
    df['Prec_15min'] = df['PrecAc'].diff()

    # O primeiro registro ficará como NaN (pois não tem linha anterior para subtrair)
    # Geralmente, preenchemos com 0.0
    df['Prec_15min'] = df['Prec_15min'].fillna(0.0)

    # Caso o total acumulado resete (volte a zero no sensor), 
    # valores negativos podem surgir. Podemos corrigi-los assim:
    df.loc[df['Prec_15min'] < 0, 'Prec_15min'] = 0.0

    #- Definir o tempo como índice para usar o resample
    df.set_index('DataHora', inplace=True)

    #- Agrupar por Hora ('h')
    # 'Cota' -> tiramos a média (mean), o 'mean' por padrão pula os NaNs (skipna=True)
    # 'Prec_15min' -> somamos (sum)
    # 'EstacaoCodigo' -> pegamos o primeiro (first) apenas para manter o código da estação
    df_hora_cheia = df.resample('h').agg({
        'Cota': 'mean',
        'Prec_15min': 'sum'
    }).reset_index()

    df_hora_cheia['EstacaoCodigo'] = codigo_nome_arquivo

    #- Ajustar ordem e nomes das colunas (ordena e renomeia)
    df_hora_cheia = df_hora_cheia[['DataHora', 'EstacaoCodigo', 'Cota', 'Prec_15min']]
    df_hora_cheia.columns = ['DataHora', 'EstacaoCodigo', 'Cota_Hora', 'Prec_Hora']

    #- remover horas onde não houve NENHUMA leitura de cota e precipitação (sensor desligado)
    df_hora_cheia = df_hora_cheia.dropna(subset=['Cota_Hora', 'Prec_Hora'], how='all')

    #< ------------------------------------------------------------------------------------------------------------------------------
    def transformar_para_banco(df_origem, col_valor, prefixo_saida):
        """ Transforma a tabela para o formato exato dos modelos MDB 
        """
        df = df_origem.copy()
        df['DataHora'] = pd.to_datetime(df['DataHora'])
        df['Data'] = df['DataHora'].dt.date
        df['Hora'] = df['DataHora'].dt.hour

        #- 1. Pivotagem e garantia de 24 colunas (00 a 23)
        df_pivot = df.pivot(index=['Data', 'EstacaoCodigo'], columns='Hora', values=col_valor)
        df_pivot = df_pivot.reindex(columns=range(24))
        
        #- Define nomes como Chuva00... ou Cota00...
        colunas_h_nome = [f'{prefixo_saida}{i:02d}' for i in range(24)]
        df_pivot.columns = colunas_h_nome
        df_pivot = df_pivot.reset_index()

        #- 2. Atributos Comuns
        df_pivot['RegistroID'] = range(len(df_pivot))
        df_pivot['Importado'] = 0
        df_pivot['Temporario'] = 0
        df_pivot['Removido'] = 0
        df_pivot['ImportadoRepetido'] = 0
        df_pivot['NivelConsistencia'] = 1
        
        #- 3. Lógica específica por tipo de tabela
        if prefixo_saida == 'Chuva':
            df_pivot['TipoMedicaoChuvas'] = 3
            # O pandas ignora NaN por padrão em sum, max, mean
            df_pivot['Maxima'] = df_pivot[colunas_h_nome].max(axis=1)
            # Usamos min_count=1 no sum() para que, se tudo for NaN, o resultado seja NaN (e não 0)
            df_pivot['Total'] = df_pivot[colunas_h_nome].sum(axis=1, min_count=1)

            #- Extração da hora do valor máximo
            #< idxmax apenas onde a linha não é toda nula
            mask = df_pivot[colunas_h_nome].notna().any(axis=1)
            df_pivot['HoraMaxima'] = np.nan
            df_pivot.loc[mask, 'HoraMaxima'] = (df_pivot.loc[mask, colunas_h_nome]
                                                .idxmax(axis=1)
                                                .str.extract(r'(\d+)')
                                                .astype(float).values)
            
            df_pivot['MaximaStatus'] = 1
            df_pivot['TotalStatus'] = 1
            
            ordem_calculos = ['TipoMedicaoChuvas', 'Maxima', 'Total', 'HoraMaxima', 'MaximaStatus', 'TotalStatus']

        else: # Caso seja 'Cota'
            df_pivot['TipoMedicaoCotas'] = 3
            df_pivot['Maxima'] = df_pivot[colunas_h_nome].max(axis=1)
            df_pivot['Minima'] = df_pivot[colunas_h_nome].min(axis=1)
            df_pivot['Media'] = df_pivot[colunas_h_nome].mean(axis=1)
            
            mask = df_pivot[colunas_h_nome].notna().any(axis=1)
            df_pivot['HoraMaxima'] = np.nan
            df_pivot['HoraMinima'] = np.nan
            
            if mask.any():
                df_pivot.loc[mask, 'HoraMaxima'] = (df_pivot.loc[mask, colunas_h_nome]
                                                    .idxmax(axis=1)
                                                    .str.extract(r'(\d+)')
                                                    .astype(float).values)
                df_pivot.loc[mask, 'HoraMinima'] = (df_pivot.loc[mask, colunas_h_nome]
                                                    .idxmin(axis=1)
                                                    .str.extract(r'(\d+)')
                                                    .astype(float).values)
            
            df_pivot['MaximaStatus'] = 1
            df_pivot['MinimaStatus'] = 1
            df_pivot['MediaStatus'] = 1
            
            ordem_calculos = ['TipoMedicaoCotas', 'Maxima', 'Minima', 'Media', 'HoraMaxima', 'HoraMinima', 'MaximaStatus', 'MinimaStatus', 'MediaStatus']

        #- 4. Datas de processamento
        hoje = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_pivot['DataIns'] = hoje
        df_pivot['DataAlt'] = hoje
        df_pivot['RespAlt'] = 1
        
        #- Adicionar colunas de Status
        colunas_status = [f'{prefixo_saida}{i:02d}Status' for i in range(24)]
        for col in colunas_status:
            df_pivot[col] = 1

        #- 5. Montagem da Ordem Final 
        colunas_base = ['RegistroID', 'Importado', 'Temporario', 'Removido', 'ImportadoRepetido', 'EstacaoCodigo', 'NivelConsistencia', 'Data']
        colunas_finais = colunas_base + ordem_calculos + colunas_h_nome + colunas_status + ['DataIns', 'DataAlt', 'RespAlt']

        return df_pivot[colunas_finais]

    #< ------------------------------------------------------------------------------------------------------------------------------
    #$ Gera os DataFrames finais
    df_chuva = transformar_para_banco(df_hora_cheia, 'Prec_Hora', 'Chuva')
    df_cota = transformar_para_banco(df_hora_cheia, 'Cota_Hora', 'Cota')
    
    return df_chuva, df_cota, codigo_nome_arquivo
#< ------------------------------------------------------------------------------------------------------------------------------

#< ------------------------------------------------------------------------------------------------------------------------------
class Tabelas():
    """ Relação de tabelas do banco de dados.\n
        Elas estão separadas por suas características
        """
    codigo_coluna_6 = ['Cotas', 'Vazoes', 'Chuvas',
                        'CurvaDescarga', 'ResumoDescarga', 'MedDescMolinete',
                        'PerfilTransversal', 'QualAgua', 'Sedimentos',
                        'Chuvas24', 'Chuvas2', 'Cotas24']
    """o código da estação está posicionado na sexta coluna"""

    relacionadas = ['PerfilTransversalVert', 'QualAguaStatus']
    """tabelas relacionadas com o ID"""

    sem_codigo = ['Bacia','SubBacia','Estado']
    """tabelas que não possuem código"""

    codigo_coluna_18 = ['Estacao']
    """o código da estação está posicionado na décima oitava coluna coluna"""
    
    todas_extensoes = (("all files","*.*"),("Excel files","*.xls"),("Excel files","*.xlsx"),("mdb files","*.mdb"))
    mdb = (("mdb files","*.mdb"),("all files","*.*"))
    excel = (("Excel files","*.xls"),("Excel files","*.xlsx"),("all files","*.*"))
    """tuplas de extensões"""

#< ------------------------------------------------------------------------------------------------------------------------------
class ExportaTabelaMDB():
    """ Importa para o MDB local a tabela do SQL
        Versão corrigida para Linux e Windows
    """
    def __init__(self, path_mdb: str, table_name: str, table_data, codigoLista: list, rids):
        self.path_mdb = path_mdb
        self.table_name = table_name
        self.table_data = table_data
        self.codigoLista = codigoLista
        self.rids = rids
        self.platform = platform.system()

    #< ------------------------------------------------------------------------------------------------------------------------------
    # MÉTODOS WINDOWS (com pyodbc) - IGUAL AO SEU ORIGINAL
    #< ------------------------------------------------------------------------------------------------------------------------------
    def conectaMDB_Windows(self) -> pyodbc.Connection:
        con_string = f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.path_mdb};"
        return pyodbc.connect(con_string)
    
    def ultimoRegistro_Windows(self, cursor):
        query = 'SELECT Max(RegistroID) AS ID FROM Identificadores;'
        cursor.execute(query)
        idmax = cursor.fetchall()
        return idmax[0][0]
    
    def querypadrao_windows(self, row, sql):
        valores_formatados = []
        for valor in row:
            if pd.isnull(valor): 
                valores_formatados.append("null")
            elif isinstance(valor, str):
                v = valor.replace("'", " ")
                valores_formatados.append(f"'{v}'")
            elif isinstance(valor, (datetime, pd.Timestamp)):
                valores_formatados.append(f'#{valor.strftime("%Y-%m-%d %H:%M")}#')
            else:
                valores_formatados.append(str(valor))
        return sql + ", ".join(valores_formatados) + ")"
    
    def exporta_dados_Windows(self):
        conn = self.conectaMDB_Windows()
        cursor = conn.cursor()
        idmax = self.ultimoRegistro_Windows(cursor)
        
        cont = idmax
        relacaoID = {}
        
        colunas = cursor.columns(table=self.table_name)
        nomes_colunas = [coluna.column_name for coluna in colunas]
        sql_base = f'INSERT INTO {self.table_name} (' + ', '.join(nomes_colunas) + ') VALUES ('
        
        if self.table_name in Tabelas.codigo_coluna_6:
            for row in self.table_data:
                if row[5] in self.codigoLista:
                    antigoID = row[0]
                    relacaoID.update({antigoID: cont})
                    row_list = list(row)
                    row_list[0] = cont
                    sql = self.querypadrao_windows(row_list, sql_base)
                    cursor.execute(sql)
                    cont += 1
        
        elif self.table_name in Tabelas.relacionadas:
            for row in self.table_data:
                if row[0] in list(self.rids.keys()):
                    row_list = list(row)
                    row_list[0] = self.rids[row_list[0]]
                    sql = self.querypadrao_windows(row_list, sql_base)
                    cursor.execute(sql)
        
        elif self.table_name in Tabelas.codigo_coluna_18:
            for row in self.table_data:
                if row[17] in self.codigoLista:
                    row_list = list(row)
                    row_list[0] = cont
                    sql = self.querypadrao_windows(row_list, sql_base)
                    cursor.execute(sql)
                    cont += 1
        
        elif self.table_name in Tabelas.sem_codigo:
            for row in self.table_data:
                row_list = list(row)
                row_list[0] = cont
                sql = self.querypadrao_windows(row_list, sql_base)
                cursor.execute(sql)
                cont += 1
        
        cursor.execute(f"UPDATE Identificadores SET RegistroID = {cont} WHERE RegistroID = {idmax}")
        conn.commit()
        conn.close()
        print(f"Tabela {self.table_name} inserida no banco MDB (Windows)")
        return relacaoID

    #< ------------------------------------------------------------------------------------------------------------------------------
    # MÉTODOS LINUX CORRIGIDOS
    #< ------------------------------------------------------------------------------------------------------------------------------
    def get_colunas_linux(self):
        """Obtém os nomes das colunas da tabela usando mdb-schema"""
        import subprocess
        import re
        
        try:
            result = subprocess.run(
                ['mdb-schema', self.path_mdb, self.table_name],
                capture_output=True, text=True
            )
            
            # Extrai nomes das colunas do schema
            colunas = []
            linhas = result.stdout.split('\n')
            for linha in linhas:
                match = re.search(r'^\s+([a-zA-Z0-9_]+)', linha)
                if match:
                    colunas.append(match.group(1))
            
            if not colunas:
                # Fallback: nomes genéricos
                if self.table_data and len(self.table_data) > 0:
                    colunas = [f"col{i}" for i in range(len(self.table_data[0]))]
            
            return colunas
        except:
            return [f"col{i}" for i in range(len(self.table_data[0]))] if self.table_data else []
    
    def ultimoRegistro_Linux(self):
        import subprocess
        import io
        import pandas as pd
        
        try:
            result = subprocess.run(
                ['mdb-export', self.path_mdb, 'Identificadores'],
                capture_output=True, text=True, check=True
            )
            
            if result.stdout.strip():
                df_ids = pd.read_csv(io.StringIO(result.stdout))
                if not df_ids.empty and 'RegistroID' in df_ids.columns:
                    idmax = df_ids['RegistroID'].max()
                    return int(idmax) if not pd.isna(idmax) else 0
            return 0
        except:
            return 0
    
    def formatar_valor_linux(self, valor):
        """Formata um valor para o formato esperado pelo MDBTools"""
        if pd.isnull(valor):
            return ''  # CSV vazio = NULL
        elif isinstance(valor, (datetime, pd.Timestamp)):
            # Formato ISO com aspas - o MDBTools entende
            return valor.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(valor, float):
            # Evita notação científica
            return f"{valor:.10f}".rstrip('0').rstrip('.') if '.' in f"{valor:.10f}" else f"{valor:.10f}"
        else:
            return str(valor)
    
    def exporta_dados_Linux(self):
        """Versão Linux corrigida - usa INSERT com colunas explícitas"""
        import subprocess
        import tempfile
        import os
        
        print(f"\n=== EXPORTANDO {self.table_name} NO LINUX ===")
        
        # Pega o último ID
        idmax = self.ultimoRegistro_Linux()
        cont = idmax
        relacaoID = {}
        registros_filtrados = []
        
        # Filtra registros (mesma lógica do Windows)
        if self.table_name in Tabelas.codigo_coluna_6:
            for row in self.table_data:
                if row[5] in self.codigoLista:
                    antigoID = row[0]
                    relacaoID.update({antigoID: cont})
                    row_list = list(row)
                    row_list[0] = cont
                    registros_filtrados.append(row_list)
                    cont += 1
        
        elif self.table_name in Tabelas.relacionadas:
            for row in self.table_data:
                if row[0] in list(self.rids.keys()):
                    row_list = list(row)
                    row_list[0] = self.rids[row_list[0]]
                    registros_filtrados.append(row_list)
        
        elif self.table_name in Tabelas.codigo_coluna_18:
            for row in self.table_data:
                if row[17] in self.codigoLista:
                    row_list = list(row)
                    row_list[0] = cont
                    registros_filtrados.append(row_list)
                    cont += 1
        
        elif self.table_name in Tabelas.sem_codigo:
            for row in self.table_data:
                row_list = list(row)
                row_list[0] = cont
                registros_filtrados.append(row_list)
                cont += 1
        
        if not registros_filtrados:
            print("Nenhum registro para inserir")
            return relacaoID
        
        # Obtém nomes das colunas
        colunas = self.get_colunas_linux()
        print(f"Colunas: {colunas}")
        
        # Cria arquivo SQL com INSERTs explícitos
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False, encoding='utf-8') as tmp:
            sql_file = tmp.name
            
            # Escreve os INSERTs
            for row in registros_filtrados:
                valores = []
                for i, valor in enumerate(row):
                    if i < len(colunas):
                        valor_formatado = self.formatar_valor_linux(valor)
                        valores.append(f"'{valor_formatado}'" if valor_formatado else "NULL")
                    else:
                        valores.append("NULL")
                
                insert_sql = f"INSERT INTO {self.table_name} ({', '.join(colunas)}) VALUES ({', '.join(valores)});\n"
                tmp.write(insert_sql)
        
        print(f"Arquivo SQL criado: {sql_file}")
        print(f"Total de INSERTs: {len(registros_filtrados)}")
        
        # Executa o SQL usando mdb-sql
        try:
            cmd = ['mdb-sql', '-i', sql_file, self.path_mdb]
            print(f"Executando: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True, text=True
            )
            
            print(f"Return code: {result.returncode}")
            if result.stderr:
                print(f"stderr: {result.stderr}")
            
            if result.returncode == 0:
                print(f"✅ Tabela {self.table_name} inserida com sucesso!")
                
                # Atualiza Identificadores
                if cont > idmax:
                    with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_up:
                        up_file = tmp_up.name
                        tmp_up.write(f"UPDATE Identificadores SET RegistroID = {cont} WHERE RegistroID = {idmax};")
                    
                    subprocess.run(['mdb-sql', '-i', up_file, self.path_mdb], capture_output=True)
                    os.unlink(up_file)
            else:
                print(f"❌ Falha ao inserir tabela {self.table_name}")
                
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            os.unlink(sql_file)
        
        return relacaoID

    #< ------------------------------------------------------------------------------------------------------------------------------
    def exporta_dados_Linux_v2(self):
        """ Versão Linux com transações explícitas """
        import subprocess
        import tempfile
        import os
        
        print(f"\n=== INICIANDO EXPORTAÇÃO LINUX V2 ===")
        print(f"Tabela: {self.table_name}")
        print(f"Registros a processar: {len(self.table_data)}")
        
        # Pega o último ID
        idmax = self.ultimoRegistro_Linux()
        print(f"ID máximo atual: {idmax}")
        
        cont = idmax
        relacaoID = {}
        registros_para_inserir = []
        
        # Filtra registros (mesmo código de antes)
        if self.table_name in Tabelas.codigo_coluna_6:
            for i, row in enumerate(self.table_data):
                if row[5] in self.codigoLista:
                    antigoID = row[0]
                    relacaoID.update({antigoID: cont})
                    row_list = list(row)
                    row_list[0] = cont
                    registros_para_inserir.append(row_list)
                    cont += 1
            print(f"Registros filtrados: {len(registros_para_inserir)}")
        
        elif self.table_name in Tabelas.relacionadas:
            for i, row in enumerate(self.table_data):
                if row[0] in list(self.rids.keys()):
                    row_list = list(row)
                    row_list[0] = self.rids[row_list[0]]
                    registros_para_inserir.append(row_list)
            print(f"Registros filtrados: {len(registros_para_inserir)}")
        
        elif self.table_name in Tabelas.codigo_coluna_18:
            for i, row in enumerate(self.table_data):
                if row[17] in self.codigoLista:
                    row_list = list(row)
                    row_list[0] = cont
                    registros_para_inserir.append(row_list)
                    cont += 1
            print(f"Registros filtrados: {len(registros_para_inserir)}")
        
        elif self.table_name in Tabelas.sem_codigo:
            for i, row in enumerate(self.table_data):
                row_list = list(row)
                row_list[0] = cont
                registros_para_inserir.append(row_list)
                cont += 1
            print(f"Registros filtrados: {len(registros_para_inserir)}")
        
        if not registros_para_inserir:
            print("Nenhum registro para inserir!")
            return relacaoID
        
        # Cria arquivo SQL com transações
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False, encoding='utf-8') as tmp:
            sql_file = tmp.name
            
            # Inicia transação
            tmp.write("BEGIN TRANSACTION;\n")
            
            # Gera INSERTs
            for row in registros_para_inserir:
                # Pega nomes das colunas (usando col0, col1, etc. como fallback)
                col_names = [f"col{i}" for i in range(len(row))]
                
                valores = []
                for valor in row:
                    if pd.isnull(valor):
                        valores.append("NULL")
                    elif isinstance(valor, str):
                        v = valor.replace("'", "''")
                        valores.append(f"'{v}'")
                    elif isinstance(valor, (datetime, pd.Timestamp)):
                        valores.append(f"'{valor.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else:
                        valores.append(str(valor))
                
                insert_sql = f"INSERT INTO {self.table_name} ({', '.join(col_names)}) VALUES ({', '.join(valores)});\n"
                tmp.write(insert_sql)
            
            # Atualiza Identificadores se necessário
            if cont > idmax:
                tmp.write(f"UPDATE Identificadores SET RegistroID = {cont} WHERE RegistroID = {idmax};\n")
            
            # Finaliza transação
            tmp.write("COMMIT;\n")
        
        print(f"Arquivo SQL criado: {sql_file}")
        print(f"Total de INSERTs: {len(registros_para_inserir)}")
        
        # Executa o SQL
        try:
            # Primeira tentativa: mdb-sql com arquivo
            cmd = ['mdb-sql', '-i', sql_file, self.path_mdb]
            print(f"Executando: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True, text=True
            )
            
            print(f"Return code: {result.returncode}")
            if result.stderr:
                print(f"stderr: {result.stderr[:500]}")
            
            if result.returncode == 0:
                print(f"✅ Tabela {self.table_name} inserida com sucesso!")
                
                # Verifica se os dados foram realmente inseridos
                verify = subprocess.run(
                    ['mdb-export', self.path_mdb, self.table_name],
                    capture_output=True, text=True
                )
                if verify.stdout:
                    linhas = verify.stdout.strip().split('\n')
                    print(f"📊 Tabela agora tem {len(linhas)} linhas")
                else:
                    print("⚠️ Tabela parece vazia após inserção")
            else:
                print(f"❌ Falha ao inserir tabela {self.table_name}")
                
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            os.unlink(sql_file)
        
        return relacaoID

    #< ------------------------------------------------------------------------------------------------------------------------------
    def exporta_dados_Linux_v3(self):
        """ Versão Linux usando mdb-import (mais confiável) """
        import subprocess
        import tempfile
        import csv
        import os
        
        print(f"\n=== INICIANDO EXPORTAÇÃO LINUX V3 ===")
        
        # Pega o último ID
        idmax = self.ultimoRegistro_Linux()
        cont = idmax
        relacaoID = {}
        registros_para_inserir = []
        
        # Filtra registros (mesmo código)
        if self.table_name in Tabelas.codigo_coluna_6:
            for row in self.table_data:
                if row[5] in self.codigoLista:
                    antigoID = row[0]
                    relacaoID.update({antigoID: cont})
                    row_list = list(row)
                    row_list[0] = cont
                    registros_para_inserir.append(row_list)
                    cont += 1
        
        # ... (outros casos)
        
        if not registros_para_inserir:
            return relacaoID
        
        # Cria arquivo CSV
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False, newline='') as tmp:
            csv_file = tmp.name
            writer = csv.writer(tmp)
            
            # Escreve dados
            for row in registros_para_inserir:
                dados = []
                for valor in row:
                    if pd.isnull(valor):
                        dados.append('')
                    elif isinstance(valor, (datetime, pd.Timestamp)):
                        dados.append(valor.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        dados.append(str(valor))
                writer.writerow(dados)
        
        # Usa mdb-import (caminho direto para o binário)
        try:
            # Encontra o caminho real do mdb-import
            which_result = subprocess.run(['which', 'mdb-import'], capture_output=True, text=True)
            mdb_import_path = which_result.stdout.strip()
            
            if not mdb_import_path:
                mdb_import_path = '/usr/bin/mdb-import'
            
            print(f"Usando mdb-import em: {mdb_import_path}")
            
            # Comando: mdb-import -d , arquivo.mdb dados.csv tabela
            cmd = [mdb_import_path, '-d', ',', self.path_mdb, csv_file, self.table_name]
            print(f"Executando: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True, text=True
            )
            
            print(f"Return code: {result.returncode}")
            if result.stderr:
                print(f"stderr: {result.stderr}")
            
            if result.returncode == 0:
                print(f"✅ Tabela {self.table_name} importada com sucesso!")
                
                # Atualiza Identificadores
                if cont > idmax:
                    with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp_up:
                        up_file = tmp_up.name
                        tmp_up.write(f"UPDATE Identificadores SET RegistroID = {cont} WHERE RegistroID = {idmax};")
                    
                    subprocess.run(['mdb-sql', '-i', up_file, self.path_mdb], capture_output=True)
                    os.unlink(up_file)
            else:
                print(f"❌ Falha na importação")
                
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            os.unlink(csv_file)
        
        return relacaoID

    #< ------------------------------------------------------------------------------------------------------------------------------
    def exporta_dados_Linux_v4(self):
        import subprocess
        import tempfile
        import os
        
        # 1. Obter o ID máximo atual
        idmax = self.ultimoRegistro_Linux()
        cont = idmax
        relacaoID = {}
        
        print(f"\n=== INICIANDO EXPORTAÇÃO LINUX V3 ===")
            
        # Pega o último ID
        idmax = self.ultimoRegistro_Linux()
        cont = idmax
        relacaoID = {}
        registros_para_inserir = []
        
        # Filtra registros (mesmo código)
        if self.table_name in Tabelas.codigo_coluna_6:
            for row in self.table_data:
                if row[5] in self.codigoLista:
                    antigoID = row[0]
                    relacaoID.update({antigoID: cont})
                    row_list = list(row)
                    row_list[0] = cont
                    registros_para_inserir.append(row_list)
                    cont += 1
        
        # ... (outros casos)

        if not registros_para_inserir:
            return relacaoID

        with tempfile.NamedTemporaryFile(mode='w+', suffix='.sql', delete=False) as tmp:
            sql_path = tmp.name
            # O mdb-sql as vezes precisa de comandos simples por linha
            for row in registros_para_inserir:
                valores = []
                for v in row:
                    if pd.isnull(v): valores.append("NULL")
                    elif isinstance(v, (datetime, pd.Timestamp)): 
                        valores.append(f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else: 
                        # Escapar aspas simples para evitar erro de sintaxe SQL
                        txt = str(v).replace("'", "''")
                        valores.append(f"'{txt}'")
                
                sql = f"INSERT INTO {self.table_name} VALUES ({', '.join(valores)});\n"
                tmp.write(sql)
            
            # Atualizar o contador de IDs globais do banco
            tmp.write(f"UPDATE Identificadores SET RegistroID = {cont} WHERE RegistroID = {idmax};\n")

        try:
            # EXECUTAR O COMANDO NO BANCO
            # -F força a execução mesmo com erros menores
            cmd = ['mdb-sql', '-F', '-p', '-i', sql_path, self.path_mdb]
            subprocess.run(cmd, capture_output=True, text=True, check=True)
        finally:
            if os.path.exists(sql_path):
                os.unlink(sql_path)
                
        return relacaoID

    #< ------------------------------------------------------------------------------------------------------------------------------
    # MÉTODO PRINCIPAL
    #< ------------------------------------------------------------------------------------------------------------------------------
    def exporta_dados_MDB(self):
        if self.platform == 'Windows':
            return self.exporta_dados_Windows()
        else:
            # return self.exporta_dados_Linux()
            return self.exporta_dados_Linux_v4()
        
#< ------------------------------------------------------------------------------------------------------------------------------
# MAIN
#< ------------------------------------------------------------------------------------------------------------------------------
# $ main (Este bloco só roda se você executar o Conversor.py diretamente para testes)

if __name__ == "__main__":
    # Para testar localmente, você precisaria definir um arquivo de teste
    #- Pega o diretório onde o script está rodando e junta com o nome do banco
    base_dir = os.path.dirname(os.path.abspath(__file__))

    csv_file = 'retr_53540001_2025.11.18.csv'
    mdb_file = os.path.join(base_dir, 'BancoTeste.mdb')
    
    if os.path.exists(csv_file):
        # 1. Chama a função que você encapsulou
        df_chuva, df_cota, codigo_nome_arquivo = processar_dados_pcd(csv_file, csv_file)

        # 2. Prepara a exportação
        tables = {'Chuvas24': df_chuva, 'Cotas24': df_cota}
        relacaoID = {} # Inicializa vazio

        for table_name in ['Chuvas24', 'Cotas24']:
            # Converte o DataFrame para lista de listas
            table_data_raw = tables[table_name].values.tolist()
            
            # Relação de códigos das estações
            codigoLista = [codigo_nome_arquivo]

            # Exporta para o MDB local
            tabelaMDB = ExportaTabelaMDB(mdb_file, table_name, table_data_raw, codigoLista, relacaoID)
            
            # Atualiza a relação de IDs
            relacaoID = tabelaMDB.exporta_dados_MDB()
    else:
        print(f"Arquivo de teste {csv_file} não encontrado.")


