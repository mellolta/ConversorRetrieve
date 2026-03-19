#!/bin/bash
set -e
echo "=== Instalando dependências para MDBTools ==="

# Atualizar e instalar pacotes base
apt-get update
apt-get install -y mdbtools unixodbc unixodbc-dev

# Verificar onde o driver MDBTools está localizado
if [ -f "/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so" ]; then
    DRIVER_PATH="/usr/lib/x86_64-linux-gnu/odbc/libmdbodbc.so"
elif [ -f "/usr/lib/libmdbodbc.so" ]; then
    DRIVER_PATH="/usr/lib/libmdbodbc.so"
elif [ -f "/usr/lib/x86_64-linux-gnu/libmdbodbc.so" ]; then
    DRIVER_PATH="/usr/lib/x86_64-linux-gnu/libmdbodbc.so"
else
    echo "Driver MDBTools não encontrado, procurando..."
    find /usr -name "*mdbodbc*" 2>/dev/null || true
    exit 1
fi

echo "Driver encontrado em: $DRIVER_PATH"

# Registrar o driver no ODBC
echo "[MDBTools]
Description = MDBTools ODBC Driver
Driver = $DRIVER_PATH
Setup = $DRIVER_PATH
FileUsage = 1
UsageCount = 1" > /etc/odbcinst.ini

echo "Driver MDBTools registrado com sucesso!"
odbcinst -q -d -n "MDBTools" || true
echo "=== Instalação concluída ==="