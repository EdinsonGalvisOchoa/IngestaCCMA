import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# CONFIGURACIÓN V3
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "raw"
BASE_PATH = "ingesta_ccma"

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="ingesta_raw", methods=["POST"])
def ingesta_raw(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Iniciando ingesta RAW")

    # 1. Leer JSON
    try:
        rows = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "El body debe ser un JSON válido",
            status_code=400
        )

    # 2. Validar que sea una lista
    if not isinstance(rows, list):
        return func.HttpResponse(
            "El body debe ser un array de registros",
            status_code=400
        )

    if len(rows) == 0:
        return func.HttpResponse(
            "El array está vacío",
            status_code=400
        )

    # 3. Validar campos obligatorios
    required_fields = {"nit", "empresa", "ciiu"}
    for i, row in enumerate(rows):
        if not required_fields.issubset(row):
            return func.HttpResponse(
                f"Registro {i} incompleto. Requiere: {required_fields}",
                status_code=400
            )

    # 4. Construir payload RAW
    now = datetime.utcnow()
    payload = {
        "metadata": {
            "ingest_id": str(uuid.uuid4()),
            "ingest_timestamp": now.isoformat(),
            "source": "powerbi-ccma",
            "records": len(rows)
        },
        "data": rows
    }

    # 5. Path particionado
    blob_path = (
        f"{BASE_PATH}/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"ingest_{now:%Y%m%d_%H%M%S}.json"
    )

    # 6. Guardar en ADLS
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
        logging.exception("Error escribiendo en ADLS")
        return func.HttpResponse(
            "Error escribiendo en almacenamiento",
            status_code=500
        )

    # 7. Respuesta OK
    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "records": len(rows),
            "path": blob_path
        }),
        mimetype="application/json",
        status_code=200
    )
