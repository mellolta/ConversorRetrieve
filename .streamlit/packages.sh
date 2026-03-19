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

# Instalar odbc-mdbtools ignorando conflitos
echo "3. Instalando odbc-mdbtools ignorando conflitos..."
apt-get install -y odbc-mdbtools -o Dpkg::Options::="--force-overwrite" || true

# Procurar pelo driver MDBTools
echo "4. Procurando pelo driver MDBTools..."
find /usr -name "libmdbodbc.so*" 2>/dev/null || true

# Criar diretórios necessários
mkdir -p /usr/lib/x86_64-linux-gnu/odbc/
mkdir -p /usr/lib/x86_64-linux-gnu/

# Copiar driver para localizações padrão
echo "5. Copiando driver para localizações padrão..."

# Procurar o driver em qualquer lugar
DRIVER_SOURCE=$(find /usr -name "libmdbodbc.so*" -type f 2>/dev/null | head -1)

if [ -n "$DRIVER_SOURCE" ]; then
    echo "Driver encontrado em: $DRIVER_SOURCE"
    
    # Copiar para todas as possíveis localizações
    cp "$DRIVER_SOURCE" /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so 2>/dev/null || true
    cp "$DRIVER_SOURCE" /usr/lib/x86_64-linux-gnu/libmdbodbc.so 2>/dev/null || true
    cp "$DRIVER_SOURCE" /usr/lib/libmdbodbc.so 2>/dev/null || true
    
    # Criar links simbólicos
    ln -sf /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so /usr/lib/libmdbodbc.so 2>/dev/null || true
    ln -sf /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so /usr/lib/x86_64-linux-gnu/libmdbodbc.so 2>/dev/null || true
    
    echo "✓ Driver copiado para locais padrão"
else
    echo "❌ Driver não encontrado no sistema"
    
    # Tentar baixar o pacote manualmente
    echo "Tentando baixar pacote odbc-mdbtools manualmente..."
    cd /tmp
    apt-get download odbc-mdbtools
    
    # Extrair o pacote
    dpkg -x odbc-mdbtools*.deb odbc-mdbtools-extract
    
    # Procurar o driver no pacote extraído
    EXTRACTED_DRIVER=$(find odbc-mdbtools-extract -name "libmdbodbc.so*" -type f 2>/dev/null | head -1)
    
    if [ -n "$EXTRACTED_DRIVER" ]; then
        echo "Driver encontrado no pacote: $EXTRACTED_DRIVER"
        
        # Copiar para locais padrão
        cp "$EXTRACTED_DRIVER" /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so
        cp "$EXTRACTED_DRIVER" /usr/lib/x86_64-linux-gnu/libmdbodbc.so
        cp "$EXTRACTED_DRIVER" /usr/lib/libmdbodbc.so
        
        echo "✓ Driver instalado manualmente"
    else
        echo "❌ Driver não encontrado no pacote"
        ls -la odbc-mdbtools-extract/usr/lib/ 2>/dev/null || true
        exit 1
    fi
    
    rm -rf odbc-mdbtools-extract
    rm -f odbc-mdbtools*.deb
fi

# Registrar o driver no ODBC
echo "6. Registrando driver MDBTools no ODBC..."

cat > /etc/odbcinst.ini << EOF
[MDBTools]
Description = MDBTools ODBC Driver
Driver = /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so
Setup = /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so
FileUsage = 1
UsageCount = 1
EOF

echo "Conteúdo do /etc/odbcinst.ini:"
cat /etc/odbcinst.ini

# Testar registro
echo "7. Testando registro do driver..."
odbcinst -q -d -n "MDBTools" && echo "✓ Driver registrado com sucesso!" || echo "❌ Falha no registro"

# Listar drivers disponíveis
echo "8. Drivers disponíveis:"
odbcinst -q -d || echo "Nenhum driver encontrado"

# Verificar arquivos instalados
echo "9. Verificando arquivos instalados:"
ls -la /usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so 2>/dev/null && echo "✓ Driver no local padrão ODBC" || echo "❌ Driver não encontrado no local ODBC"
ls -la /usr/lib/x86_64-linux-gnu/libmdbodbc.so 2>/dev/null && echo "✓ Driver no local x86_64" || echo "❌ Driver não encontrado no local x86_64"
ls -la /usr/lib/libmdbodbc.so 2>/dev/null && echo "✓ Driver no local /usr/lib" || echo "❌ Driver não encontrado no /usr/lib"

echo "=== INSTALAÇÃO CONCLUÍDA ==="