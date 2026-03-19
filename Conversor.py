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
    """
    def __init__(self, path_mdb: str, table_name: str, table_data, codigoLista: list, rids):
        """ Parâmetros de entrada:
            - path_mdb: endereço do banco
            - table_name: nome da tabela
            - table_data: tabela capturada do SQL
            - codigoLista: relação de códigos das estações retirados da planilha
            - rids: relação entre os IDs antigos e novos (apenas para tabelas relacionadas)
        """
        self.path_mdb = path_mdb
        self.table_name = table_name
        self.table_data = table_data
        self.codigoLista = codigoLista
        self.rids = rids

    #< ------------------------------------------------------------------------------------------------------------------------------
    # def conectaMDB(self) -> pyodbc.Connection:
    #     """ Conecta com o MDB local
    #         - cria a conexão
    #     """
    #     #- Endereço de conexão 
    #     con_string = ''.join(["DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=",self.path_mdb])

    #     #- Criando a conexão
    #     conn = pyodbc.connect(con_string)
    #     # print("-----\nConnected to Local Database\n-----")  #! print

    #     return conn
    
    def conectaMDB(self) -> pyodbc.Connection:
        """ Conecta com o MDB local adaptado para Windows ou Linux """
        import platform
        
        if platform.system() == 'Windows':
            driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
        else:
            # No Linux (Streamlit Cloud), o driver instalado pelo odbc-mdbtools 
            # geralmente é registrado com este nome exato:
            driver = "MDBTools"
            
        con_string = f"Driver={driver};DBQ={self.path_mdb};"
        return pyodbc.connect(con_string)
    
    #< ------------------------------------------------------------------------------------------------------------------------------
    # def querypadrao(self, row, sql: str):
    #     valores_formatados = []
    #     for valor in row:
    #         # Prioridade 1: Tratar nulos reais (None e NaN)
    #         if pd.isnull(valor): 
    #             valores_formatados.append("null")
    #         # Prioridade 2: Strings com escape de aspas
    #         elif isinstance(valor, str):
    #             v = valor.replace("'", " ")
    #             valores_formatados.append(f"'{v}'")
    #         # Prioridade 3: Datas no formato que você já validou
    #         elif isinstance(valor, (datetime, pd.Timestamp)):
    #             valores_formatados.append(f'#{valor.strftime("%Y-%m-%d %H:%M")}#')
    #         else:
    #             valores_formatados.append(str(valor))
                
    #     sql = sql + ", ".join(valores_formatados) + ")"
    #     # Garantia final para qualquer "None" que tenha escapado como string
    #     return sql.replace('None', 'null')

    def querypadrao(self, row, sql: str):
        import platform
        
        valores_formatados = []
        
        for valor in row:
            if pd.isnull(valor):
                # Linux (MDBTools) prefere NULL, Windows aceita null
                if platform.system() == 'Windows':
                    valores_formatados.append("null")
                else:
                    valores_formatados.append("NULL")
                    
            elif isinstance(valor, str):
                v = valor.replace("'", " ")
                valores_formatados.append(f"'{v}'")
                
            elif isinstance(valor, (datetime, pd.Timestamp)):
                # Mantém exatamente o formato original que funcionava
                data_str = valor.strftime("%Y-%m-%d %H:%M")
                
                if platform.system() == 'Windows':
                    # Windows Access: formato com #
                    valores_formatados.append(f'#{data_str}#')
                else:
                    # Linux MDBTools: pode precisar de aspas
                    valores_formatados.append(f"'{data_str}'")
                    
            else:
                valores_formatados.append(str(valor))
        
        # Para Linux, garantir NULL em maiúsculo
        sql_final = sql + ", ".join(valores_formatados) + ")"
        
        if platform.system() != 'Windows':
            sql_final = sql_final.replace('null', 'NULL')
            sql_final = sql_final.replace('None', 'NULL')
        
        return sql_final
    
    #< ------------------------------------------------------------------------------------------------------------------------------
    # def ultimoRegistro(self, cursor:pyodbc.Connection):
    #     #- Buscando o valor do RegistroID
    #     query = 'SELECT Max(RegistroID) AS ID FROM Identificadores;'
    #     cursor.execute(query)
    #     idmax = cursor.fetchall()
    #     idmax = idmax[0][0]
    #     cont = idmax
    #     # print("Último RegistroID: ",cont) #! print

    #     return cont
    
    def ultimoRegistro(self, cursor:pyodbc.Cursor):
        import platform
        
        try:
            if platform.system() == 'Windows':
                query = 'SELECT Max(RegistroID) AS ID FROM Identificadores;'
            else:
                # Linux MDBTools pode não gostar do ponto e vírgula
                query = 'SELECT MAX(RegistroID) AS ID FROM Identificadores'
                
            cursor.execute(query)
            row = cursor.fetchone()
            
            if row and row[0] is not None:
                cont = row[0]
            else:
                cont = 0
                
            return cont
            
        except Exception as e:
            print(f"Erro ao buscar último registro: {e}")
            # Se falhar, tenta uma abordagem mais simples
            try:
                cursor.execute("SELECT COUNT(*) FROM Identificadores")
                count = cursor.fetchone()[0]
                return count  # Não é ideal, mas pode funcionar como fallback
            except:
                return 0
            
    #< ------------------------------------------------------------------------------------------------------------------------------
    def insercao_dos_dados(self, cursor:pyodbc.Cursor, idmax:int):
        """ Criação da linha de comando SQL para a inserção dos dados no banco
        """
        #- contador de ID
        cont = idmax

        #- captura o nome das colunas
        colunas = cursor.columns(table=self.table_name)

        #- parte inicial do comando SQL
        sql_base = f'INSERT INTO {self.table_name} ('

        # print([nome.column_name for nome in cursor.columns(table=table_name)])    #! consome o comando SQL
        # n = 0
        #- insere o nome de cada coluna no comando SQL
        for coluna in colunas:
            # print(n, coluna.column_name)    #! imprime as colunas de gravação
            # n += 1
            sql_base = sql_base + f'{coluna.column_name},'
        sql_base = sql_base[:-1] + ') VALUES ('             #< sql_base[:-1] para retirar a virgula

        relacaoID = {}                                      #- receberá a relação dos IDs antigos e novos

        #% apenas tabelas cujo código esteja na sexta coluna
        if self.table_name in Tabelas.codigo_coluna_6:
            for row in self.table_data:
                #- insere apenas as linhas cujo código esteja na lista 
                if row[5] in self.codigoLista:              #- a tabela deve conter o código da estação na sexta coluna
                    antigoID = row[0]                       #- armazena o antigo ID
                    relacaoID.update({antigoID:cont})       #- dicionário para correlacionar os IDs antigos com os novos
                    sql = sql_base
                    row = list(row)
                    row[0] = cont                           #- recebe novo número para o RegistroID

                    sql = self.querypadrao(row, sql)

                    cursor.execute(sql)
                    cont = cont + 1

        #% apenas as tabelas cujo Id está relacionado as tabelas_codigo_coluna_6
        if self.table_name in Tabelas.relacionadas:
            for row in self.table_data:
                if row[0] in list(self.rids.keys()):        #- executa apenas para os IDs da relação
                    sql = sql_base
                    row = list(row)
                    row[0] = self.rids[row[0]]              #: recebe o RegistroID da relação => self.rids.values()
                    
                    sql = self.querypadrao(row, sql)

                    cursor.execute(sql)

        #% apenas tabelas cujo código esteja na décima oitava coluna
        if self.table_name in Tabelas.codigo_coluna_18:
            for row in self.table_data:
                #- insere apenas as linhas cujo código esteja na lista 
                if row[17] in self.codigoLista:             #- a tabela deve conter o código da estação na décima oitava coluna
                    sql = sql_base
                    row = list(row)
                    row[0] = cont                           #- recebe novo número para o RegistroID
                    
                    sql = self.querypadrao(row, sql)

                    cursor.execute(sql)
                    cont = cont + 1

        #% importa todas as linhas da tabela sem filtro
        if self.table_name in Tabelas.sem_codigo:
            for row in self.table_data:
            #- insere todas as linhas da tabela 
                sql = sql_base
                row = list(row)
                row[0] = cont                               #- recebe novo número para o RegistroID
                
                sql = self.querypadrao(row, sql)

                cursor.execute(sql)
                cont = cont + 1

        #- Atualizar o número de registros na base
        sql =f"UPDATE Identificadores SET RegistroID = {cont} WHERE RegistroID = {idmax}"
        cursor.execute(sql)

        return relacaoID

    #< ------------------------------------------------------------------------------------------------------------------------------
    def exporta_dados_MDB(self):
        """ Importa para o MDB local a tabela do SQL
            - conecta ao banco local
            - importa a tabela informada
            - desconecta
        """
        #- faz a conexão com o banco
        conn = self.conectaMDB()

        #- Cursor para a inserção de operações SQL
        cursor = conn.cursor()

        #- resgata o ID do último registro
        idmax = self.ultimoRegistro(cursor)

        #- comando SQL para a inserção no banco de dados
        relacaoID = self.insercao_dos_dados(cursor, idmax)

        conn.commit()
        # print(f"Tabela \033[90m{self.table_name}\033[0m inserida no banco MDB")   #! print
        print(f"Tabela {self.table_name} inserida no banco MDB")   #! print
        #< ------------------------------------------------------------------------------------------------------------------------------
        #- Fecha a conexão
        conn.close()
        # print("-----\nDisconnected from Local Database\n-----") #! print

        return relacaoID

#< ------------------------------------------------------------------------------------------------------------------------------
#$ main

# #- Dicionário com os DataFrames gerados
# tables = {'Chuvas24': df_chuva, 
#            'Cotas24': df_cota}
# relacaoID = {} # Inicializa vazio

# for table_name in ['Chuvas24', 'Cotas24']:
#     # 1. Converte o DataFrame para lista de listas (essencial para o pyodbc)
#     table_data_raw = tables[table_name].values.tolist()
    
#     #- relação do antigo ID com o novo (usado apenas em tabelas relacionadas)
#     rids = relacaoID

#     #- relação de códigos das estações retirados da planilha
#     codigoLista = [codigo_nome_arquivo]

#     #- exporta para o MDB local a tabela extraída do SQL
#     tabelaMDB = ExportaTabelaMDB(mdb_file, table_name, table_data_raw, codigoLista, rids)

#     #- Atualiza a relação de IDs para a próxima tabela usar se necessário
#     relacaoID = tabelaMDB.exporta_dados_MDB()

# < ------------------------------------------------------------------------------------------------------------------------------
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


