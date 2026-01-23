# Data Engineering Challenge: Document Platform ELT Pipeline

Este repositorio contiene una solución integral de ingeniería de datos para el procesamiento de logs de una plataforma colaborativa de documentos. El proyecto implementa un pipeline **ELT (Extract, Load, Transform)** utilizando una **Arquitectura de Medallón** (Bronze, Silver, Gold), garantizando la limpieza, integridad y disponibilidad de los datos para analítica avanzada.



## 🚀 Stack Tecnológico

* **Lenguaje:** Python 3.8+
* **Validación de Datos:** Pydantic
* **Orquestación:** Apache Airflow
* **Transformación de Datos:** dbt (Data Build Tool) con PostgreSQL
* **Base de Datos:** PostgreSQL
* **Infraestructura:** Docker & Docker Compose
* **Conectividad:** SQLAlchemy

---

## 🏗️ Arquitectura del Pipeline

El flujo de datos se organiza en tres capas incrementales dentro de PostgreSQL:

### 1. Capa Bronze (Raw/Staging)
* **Fuente:** Los logs se extraen de un archivo JSON localizado en `data/raw/mock_event_logs.json`.
* **Validación:** Se utiliza **Pydantic** (`src/schemas/parse_json.py`) para asegurar que cada evento cumpla con el esquema esperado antes de la ingesta.
* **Carga:** Los datos se cargan de forma incremental en la tabla `public.raw_event_logs` utilizando una columna de tipo `JSONB` para mantener la flexibilidad del dato original.

### 2. Capa Silver (Normalized)
* **Proceso:** dbt toma los datos de la capa Bronze y los distribuye en tablas relacionales normalizadas.
* **Modelos:** Se generan 4 tablas específicas filtradas por tipo de evento:
    * `silver_comments`: Atributos de comentarios.
    * `silver_shares`: Información de documentos compartidos.
    * `silver_edits`: Volumen y detalle de ediciones.
    * `silver_logins`: Registros de inicio de sesión.
* **Estrategia:** Se utiliza una estrategia de carga **incremental** con `merge` para optimizar el rendimiento y evitar duplicados.

### 3. Capa Gold (Business & Analytics)
* **Proceso:** Consolidación de métricas de negocio y KPIs.
* **Tablas Finales:**
    * `daily_active_users (DAU)`: Usuarios únicos activos en los últimos 30 días.
    * `average_session_duration_by_user`: Tiempo promedio de sesión calculado por la diferencia entre el primer y último evento diario.
    * `top_10_most_edited_documents`: Ranking de documentos según el volumen acumulado de `edit_length`.
    * `number_of_shared_documents_by_user`: Total de acciones de colaboración iniciadas por usuario.
    * `anomaly_analytics`: Detección de "Eventos Huérfanos" (acciones realizadas sin un registro de login previo).



---

## 🛠️ Instrucciones para Montar el Proyecto

Sigue estos pasos para desplegar el entorno completo utilizando Docker:

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/matiasfeliu92/ChallengeValidacionTecnicaGenAI.git](https://github.com/matiasfeliu92/ChallengeValidacionTecnicaGenAI.git)
    cd ChallengeValidacionTecnicaGenAI
    ```

2.  **Configurar variables de entorno:**
    * Crear un archivo `.env` en la raíz del proyecto.
    * Copiar y adaptar las credenciales del archivo `.env.example`.

3.  **Inicializar la base de datos de Airflow:**
    ```bash
    docker compose up --build airflow-init
    ```

4.  **Levantar los servicios (Airflow, Postgres, dbt):**
    ```bash
    docker compose up --build -d
    ```

5.  **Acceder a la interfaz de Airflow:**
    * URL: `http://localhost:8080`
    * **Usuario:** `admin`
    * **Contraseña:** `admin`

6.  **Ejecutar el Pipeline:**
    * Buscar el DAG llamado **`ChallengeValidacionTecnicaGenAI`**.
    * Activar el DAG y ejecutarlo manualmente (Trigger).

---

## 📈 Análisis de Anomalías
Dentro de la Capa Gold, el modelo `anomaly_analytics` identifica registros inconsistentes donde los usuarios interactúan con documentos sin haber disparado un evento de `user_login`. Este análisis permite identificar posibles fallos en la captura de logs de la aplicación o latencias en el envío de eventos críticos.



---

## 📂 Estructura del Repositorio
* `/dags`: Definición del flujo de trabajo en Airflow.
* `/models`: Modelos de dbt para las capas Staging, Silver y Gold.
* `/src`: Scripts de Python para extracción, carga y validación (Pydantic).
* `/data`: Archivos fuente de logs (Raw).

**Desarrollado por:** [Matias Feliu]