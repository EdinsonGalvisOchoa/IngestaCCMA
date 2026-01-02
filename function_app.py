import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# CONFIGURACIÓN (App Settings en Azure)
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "raw"
BASE_PATH = "ingesta_ccma"

# x-api-key propia (opcional)
EXPECTED_X_API_KEY = os.getenv("API_REQUEST_KEY")  # ponla en App Settings

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="ingesta_raw", methods=["POST"])
def ingesta_raw(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Iniciando ingesta RAW")

    # 0) Validación x-api-key (opcional)
    if EXPECTED_X_API_KEY:
        client_key = req.headers.get("x-api-key")
        if client_key != EXPECTED_X_API_KEY:
            return func.HttpResponse("No autorizado", status_code=401)

    # 1) Leer body como JSON (array directo)
    try:
        raw = req.get_body().decode("utf-8")
        data = json.loads(raw)
    except Exception:
        return func.HttpResponse(
            "El body debe ser un JSON válido (ARRAY). Ej: [ {...}, {...} ]",
            status_code=400
        )

    # 2) Validar que sea un array
    if not isinstance(data, list):
        return func.HttpResponse(
            "El body debe ser un ARRAY JSON. Ej: [ {...}, {...} ]",
            status_code=400
        )

    if len(data) == 0:
        return func.HttpResponse(
            "El array no puede venir vacío",
            status_code=400
        )

    # 3) Validar campos obligatorios en cada elemento
    required_fields = ["nit", "empresa", "ciiu"]
    invalid_items = []

    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            invalid_items.append({"index": idx, "error": "Cada elemento debe ser un objeto JSON"})
            continue

        missing = [f for f in required_fields if f not in item]
        if missing:
            invalid_items.append({"index": idx, "missing": missing})

    if invalid_items:
        return func.HttpResponse(
            json.dumps({
                "error": "Hay elementos inválidos en el array",
                "invalid_items": invalid_items[:50]  # para no devolver gigante
            }, ensure_ascii=False),
            status_code=400,
            mimetype="application/json"
        )

    # 4) Construir payload RAW (metadata + array completo)
    now = datetime.utcnow()
    ingest_id = str(uuid.uuid4())

    payload = {
        "metadata": {
            "ingest_id": ingest_id,
            "ingest_timestamp": now.isoformat() + "Z",
            "source": "api-ingesta-ccma",
            "records_count": len(data)
        },
        "data": data
    }

    # 5) Path particionado
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    filename = f"ingest_{now.strftime('%Y%m%d_%H%M%S')}_{ingest_id[:8]}.json"
    blob_path = (
        f"{BASE_PATH}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{filename}"
    )

    # 6) Validar settings storage
    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        return func.HttpResponse(
            "Faltan variables de entorno STORAGE_ACCOUNT_NAME / STORAGE_ACCOUNT_KEY",
            status_code=500
        )

    # 7) Subir UN SOLO archivo al RAW
    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )

        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_path
        )

        blob_client.upload_blob(
            json.dumps(payload, ensure_ascii=False),
            overwrite=True
        )

    except Exception as e:
        logging.exception("Error escribiendo en ADLS/Blob")
        return func.HttpResponse(
            f"Error escribiendo en almacenamiento: {str(e)}",
            status_code=500
        )

    logging.info(f"OK: ingest_id={ingest_id} records={len(data)} path={blob_path}")

    # 8) Respuesta OK
    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "message": "Archivo almacenado en RAW",
            "records": len(data),
            "path": blob_path,
            "ingest_id": ingest_id
        }, ensure_ascii=False),
        mimetype="application/json",
        status_code=200
    )
