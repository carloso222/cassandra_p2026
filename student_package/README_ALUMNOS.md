# Guía para alumnos

Sistema de logística con Cassandra: órdenes, productos y envíos.

## Requisitos previos

- Python 3.8 o superior
- Docker (para Cassandra)
- El ejecutable `validate` en la carpeta `student_package/` (incluido en el paquete)

## 1. Configurar el entorno

### Instalar dependencias de Python

```bash
# Crear y activar entorno virtual (recomendado)

# Linux / Mac:
python3 -m venv venv
source venv/bin/activate

# Windows:
python3 -m venv venv
.\venv\Scripts\Activate.ps1

# Instalar dependencias del proyecto
pip install -r requirements.txt
```

### Iniciar Cassandra

```bash
# Crear y arrancar el contenedor (primera vez)
docker run --name logistics -p 9042:9042 -d cassandra

# Si ya existe el contenedor, solo iniciarlo
docker start logistics
```

Espera unos segundos después de `docker start` para que Cassandra esté listo.

## 2. Ejecutar la aplicación

```bash
python3 app.py
```

La aplicación muestra un menú interactivo:

- **0**: Poblar datos de ejemplo
- **1**: Órdenes por cliente
- **2**: Productos por orden
- **3–7**: Consultas de envíos
- **8**: Cambiar email
- **9**: Salir

## 3. Validar tu implementación

Ejecuta el validador desde la carpeta del proyecto (donde están `app.py` y `model.py`):

```bash
# Validar y ver el resultado en pantalla
./student_package/validate

# En Windows:
.\student_package\validate.exe
```

**Importante**: Ejecuta desde la carpeta raíz del proyecto. La carpeta `student_package/` debe estar junto a `app.py` y `model.py`.

### Guardar el reporte para entrega

```bash
./student_package/validate --output report.txt
```

Se genera `report.txt` con el resultado y un hash para verificar que no fue modificado.

### Verificar integridad del reporte

```bash
./student_package/validate --verify report.txt
```

## 4. Interpretar el resultado

El validador muestra algo como:

```
=== Logistics App Report ===
Passed: 10/10

PASS - Option 0 - Populate sample data
PASS - Option 1 - Orders by customer
...
SHA256:abc123...
```

- **Passed: X/10**: Número de opciones correctas.
- **PASS**: La opción cumple la validación.
- **FAIL**: La opción falla (aparece el motivo).
- **SHA256**: Hash para comprobar que el reporte no fue alterado.

## 5. Solución de problemas

| Problema | Posible solución |
|----------|------------------|
| "Could not connect to Cassandra" | Revisa que Docker esté en marcha y `logistics` corriendo. |
| "No module named 'cassandra'" | Ejecuta `pip install -r requirements.txt`. |
| "./student_package/validate: Permission denied" | En Linux/Mac: `chmod +x student_package/validate`. |
| "No orders found" | Ejecuta primero la opción 0 en la app para poblar datos. |

## Archivos del proyecto

| Archivo | Descripción |
|---------|-------------|
| `app.py` | Aplicación principal (menú). |
| `model.py` | Modelo de datos y consultas a Cassandra. |
| `requirements.txt` | Dependencias de Python. |
| `student_package/validate` | Validador (no modificar). |
