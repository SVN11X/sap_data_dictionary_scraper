# 📊 SAP Data Dictionary Scraper

Extracción automatizada del diccionario ABAP desde [sapdatasheet.org](https://www.sapdatasheet.org/abap/tabl/), con dashboard web en GitHub Pages y actualización semanal via GitHub Actions.

## 🏗️ Arquitectura

```
┌──────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  GitHub Actions      │────▶│  Python Scraper      │────▶│  data/*.json        │
│  (cron: dom 03:00)   │     │  (httpx + bs4)       │     │  (committed to repo)│
└──────────────────────┘     └─────────────────────┘     └────────┬────────────┘
                                                                   │
                                                           ┌───────▼───────────┐
                                                           │  GitHub Pages      │
                                                           │  Dashboard (HTML)  │
                                                           │  + Downloads       │
                                                           └───────────────────┘
```

## 📂 Estructura del Repositorio

```
.
├── index.html                         # Dashboard web (GitHub Pages)
├── data/
│   ├── metadata.json                  # Stats y timestamps de la última extracción
│   ├── sap_tables.json                # Catálogo de tablas DDIC
│   ├── sap_fields.json                # Estructura de campos por tabla
│   └── sap_status.json                # Estado de scraping por tabla
├── scripts/
│   └── scraper.py                     # Scraper Python standalone
├── .github/
│   └── workflows/
│       └── scrape.yml                 # GitHub Actions workflow
├── requirements.txt
└── README.md
```

## 🚀 Despliegue

### 1. Crear repositorio en GitHub

```bash
git init
git add .
git commit -m "Initial commit: SAP Data Dictionary Scraper"
git remote add origin https://github.com/TU-USUARIO/sap-data-dictionary-scraper.git
git push -u origin main
```

### 2. Activar GitHub Pages

1. Ve a **Settings** → **Pages**
2. Source: **GitHub Actions**
3. El workflow `scrape.yml` se encargará del deploy automático

### 3. Permisos del workflow

1. Ve a **Settings** → **Actions** → **General**
2. En **Workflow permissions**, selecciona **Read and write permissions**
3. Marca **Allow GitHub Actions to create and approve pull requests**

### 4. Primera ejecución

- El scraper se ejecuta automáticamente cada **domingo a las 03:00 UTC**
- Para ejecutarlo manualmente: **Actions** → **SAP Data Dictionary Scraper** → **Run workflow**

## ⚙️ Opciones del Scraper

| Parámetro | Default | Descripción |
|---|---|---|
| `--max-tables N` | `0` (todos) | Limitar número de tablas a procesar |
| `--delay SECONDS` | `1.5` | Delay entre requests HTTP |
| `--skip-fields` | `false` | Solo descubrir catálogo, no extraer campos |
| `--resume` | `true` | Continuar desde datos existentes |

### Ejecución local

```bash
pip install -r requirements.txt
python scripts/scraper.py --max-tables 100 --delay 2
```

## 📥 Formatos de Descarga

El dashboard ofrece descarga en múltiples formatos:

| Formato | Descripción |
|---|---|
| **JSON** | Formato nativo, ideal para programadores |
| **CSV** | Compatible con Excel, Google Sheets, etc. |
| **Excel (.xlsx)** | Con columnas auto-dimensionadas |
| **TSV** | Tab-separated, para herramientas CLI |
| **SQL** | Sentencias INSERT para importar a cualquier BD |

## 📊 Datos Extraídos

### `sap_tables.json` — Catálogo de Tablas

| Campo | Descripción |
|---|---|
| `table_name` | Nombre de la tabla SAP (ej: MARA, BKPF) |
| `description` | Descripción de la tabla |
| `table_category` | Categoría (TRANSP, CLUSTER, POOL, etc.) |
| `delivery_class` | Clase de entrega (A, C, E, L, etc.) |
| `table_url` | URL de origen en sapdatasheet.org |

### `sap_fields.json` — Campos/Columnas

| Campo | Descripción |
|---|---|
| `table_name` | Tabla a la que pertenece |
| `position` | Posición del campo en la tabla |
| `field_name` | Nombre del campo |
| `data_element` | Elemento de datos ABAP |
| `domain` | Dominio ABAP |
| `data_type` | Tipo ABAP (CHAR, DEC, DATS, etc.) |
| `length` | Longitud |
| `decimals` | Decimales |
| `description` | Descripción del campo |
| `check_table` | Tabla de verificación |

## ⚠️ Notas

- **ECC vs S/4HANA:** El catálogo de sapdatasheet.org incluye ambos — S/4HANA es un superconjunto de ECC
- **Reanudación:** Si el scraper se interrumpe, al re-ejecutar detecta tablas ya procesadas y continúa
- **Rate limiting:** El scraper incluye delays configurables para respetar el sitio
- **Tamaño:** La extracción completa puede generar archivos de varios cientos de MB

## 📄 Licencia

Este proyecto es para uso educativo e interno. Los datos son propiedad de SAP SE.
sapdatasheet.org es un recurso comunitario independiente.
