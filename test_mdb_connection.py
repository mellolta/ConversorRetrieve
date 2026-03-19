import pyodbc
import os
import platform

def test_connection(mdb_path):
    print(f"Testando conexão com: {mdb_path}")
    print(f"Sistema: {platform.system()}")
    
    if not os.path.exists(mdb_path):
        print(f"Arquivo não encontrado: {mdb_path}")
        return
    
    if platform.system() == 'Windows':
        conn_str = f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};"
    else:
        # Tenta diferentes formas de conexão
        conn_strs = [
            f"Driver={{MDBTools}};DBQ={mdb_path};",
            f"Driver=/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so;DBQ={mdb_path};",
            f"Driver=/usr/lib/libmdbodbc.so;DBQ={mdb_path};"
        ]
    
    for i, conn_str in enumerate(conn_strs):
        print(f"\nTentativa {i+1}: {conn_str}")
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Identificadores")
            count = cursor.fetchone()[0]
            print(f"✓ Conexão bem-sucedida! Total de registros: {count}")
            conn.close()
            return True
        except Exception as e:
            print(f"✗ Falhou: {e}")
    
    return False

if __name__ == "__main__":
    test_connection("BancoTeste.mdb")