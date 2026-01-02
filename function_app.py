import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Configuracion (Azure Application settings)
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
API_REQUEST_KEY = os.getenv("API_REQUEST_KEY")  # x-api-key server-side

CONTAINER_NAME = "raw"
BASE_PATH = "ingesta_ccma"

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="ingesta_raw", methods=["POST"])
def ingesta_raw(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Iniciando ingesta RAW (batch -> single file)")


    # VALIDAR x-api-key

    client_key = req.headers.get("x-api-key")
    if not API_REQUEST_KEY:
        logging.error("API_REQUEST_KEY no configurada")
        return func.HttpResponse("API_REQUEST_KEY no configurada", status_code=500)

    if not client_key or client_key != API_REQUEST_KEY:
        logging.warning("Unauthorized: x-api-key inválida o faltante")
        return func.HttpResponse("Unauthorized", status_code=401)


    # Validar Storage config

    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        logging.error("STORAGE_ACCOUNT_NAME o STORAGE_ACCOUNT_KEY no configuradas")
        return func.HttpResponse("Storage no configurado", status_code=500)


    # 1) Leer JSON

    try:
        payload_in = req.get_json()
    except ValueError:
        return func.HttpResponse("El body debe ser un JSON válido", status_code=400)


    # 2) Validar que venga {"body":[...]}

    rows = payload_in.get("body")
    if not isinstance(rows, list) or len(rows) == 0:
        return func.HttpResponse(
            "Se espera un JSON con 'body' como lista no vacía. Ej: {'body':[...]}",
            status_code=400
        )


    # 3) Validación básica de campos (sin recorrer pesado)
    #    (opcional: valida solo primeras N filas para rendimiento)

    required_fields = {"nit", "empresa", "ciiu"}
    sample_size = min(50, len(rows))  # valida primeras 50
    for i in range(sample_size):
        if not isinstance(rows[i], dict):
            return func.HttpResponse(f"Fila {i} no es un objeto JSON", status_code=400)
        missing = [f for f in required_fields if f not in rows[i]]
        if missing:
            return func.HttpResponse(f"Fila {i} sin campos: {missing}", status_code=400)


    # 4) Construir payload RAW (metadata + data)

    now = datetime.utcnow()
    batch_id = str(uuid.uuid4())

    payload_out = {
        "metadata": {
            "batch_id": batch_id,
            "ingest_timestamp": now.isoformat(),
            "source": "api-ingesta-ccma",
            "records": len(rows)
        },
        "data": rows
    }

    # 5) Path particionado

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    filename = f"batch_{now.strftime('%Y%m%d_%H%M%S')}_{batch_id}.json"
    blob_path = (
        f"{BASE_PATH}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{filename}"
    )


    # 6) Subir un solo archivo

    try:
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=STORAGE_ACCOUNT_KEY
        )

        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_path
        )

        # IMPORTANT: ensure_ascii=False para ñ/tildes
        blob_client.upload_blob(
            json.dumps(payload_out, ensure_ascii=False),
            overwrite=True
        )

    except Exception as e:
        logging.exception("Error escribiendo en almacenamiento")
        return func.HttpResponse(
            f"Error escribiendo en almacenamiento: {str(e)}",
            status_code=500
        )


    # 7) Respuesta

    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "message": "Batch almacenado en RAW",
            "batch_id": batch_id,
            "records": len(rows),
            "path": blob_path
        }, ensure_ascii=False),
        mimetype="application/json",
        status_code=200
    )
