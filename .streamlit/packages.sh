#!/bin/bash
set -e
echo "=== INÍCIO DA INSTALAÇÃO ==="
echo "Data: $(date)"
echo "Sistema: $(uname -a)"

# Atualizar pacotes
echo "1. Atualizando lista de pacotes..."
apt-get update

# Instalar pacotes necessários
echo "2. Instalando mdbtools, unixodbc e unixodbc-dev..."
apt-get install -y mdbtools unixodbc unixodbc-dev

# Procurar pelo driver MDBTools
echo "3. Procurando pelo driver MDBTools..."
find /usr -name "*mdbodbc*" 2>/dev/null || true

# Verificar localizações comuns
echo "4. Verificando localizações comuns:"

# Localização 1: /usr/lib/x86_64-linux-gnu/odbc/
if [ -d "/usr/lib/x86_64-linux-gnu/odbc/" ]; then
    echo "Conteúdo de /usr/lib/x86_64-linux-gnu/odbc/:"
    ls -la /usr/lib/x86_64-linux-gnu/odbc/ || true
fi

# Localização 2: /usr/lib/
if [ -f "/usr/lib/libmdbodbc.so" ]; then
    echo "✓ Driver encontrado em /usr/lib/libmdbodbc.so"
    DRIVER_PATH="/usr/lib/libmdbodbc.so"
elif [ -f "/usr/lib/x86_64-linux-gnu/libmdbodbc.so" ]; then
    echo "✓ Driver encontrado em /usr/lib/x86_64-linux-gnu/libmdbodbc.so"
    DRIVER_PATH="/usr/lib/x86_64-linux-gnu/libmdbodbc.so"
elif [ -f "/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so" ]; then
    echo "✓ Driver encontrado em /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so"
    DRIVER_PATH="/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so"
else
    echo "❌ Driver não encontrado nas localizações comuns."
    echo "Tentando instalar odbc-mdbtools sem conflitos..."
    
    # Tentar baixar e extrair o pacote manualmente
    cd /tmp
    apt-get download odbc-mdbtools
    dpkg -x odbc-mdbtools*.deb odbc-mdbtools-extract
    
    echo "Procurando driver no pacote extraído:"
    find odbc-mdbtools-extract -name "*mdbodbc*" 2>/dev/null || true
    
    # Tentar copiar se encontrar
    EXTRACTED_DRIVER=$(find odbc-mdbtools-extract -name "libmdbodbc.so" -type f | head -1)
    if [ -n "$EXTRACTED_DRIVER" ]; then
        echo "✓ Driver encontrado em: $EXTRACTED_DRIVER"
        mkdir -p /usr/lib/x86_64-linux-gnu/odbc/
        cp "$EXTRACTED_DRIVER" /usr/lib/x86_64-linux-gnu/odbc/
        DRIVER_PATH="/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so"
        echo "✓ Driver copiado para: $DRIVER_PATH"
    else
        echo "❌ Não foi possível encontrar o driver no pacote extraído"
        exit 1
    fi
    
    rm -rf odbc-mdbtools-extract
    rm -f odbc-mdbtools*.deb
fi

# Registrar o driver
echo "5. Registrando driver MDBTools no ODBC..."
if [ -n "$DRIVER_PATH" ]; then
    echo "[MDBTools]
Description = MDBTools ODBC Driver
Driver = $DRIVER_PATH
Setup = $DRIVER_PATH
FileUsage = 1
UsageCount = 1" > /etc/odbcinst.ini
    
    echo "Conteúdo do /etc/odbcinst.ini:"
    cat /etc/odbcinst.ini
    
    # Testar se o driver está registrado
    echo "6. Testando registro do driver..."
    odbcinst -q -d -n "MDBTools" && echo "✓ Driver registrado com sucesso!" || echo "❌ Falha no registro"
else
    echo "❌ DRIVER_PATH não definido"
    exit 1
fi

echo "7. Verificando permissões do driver..."
ls -la $DRIVER_PATH || true
file $DRIVER_PATH || true
ldd $DRIVER_PATH || true

echo "=== INSTALAÇÃO CONCLUÍDA ==="