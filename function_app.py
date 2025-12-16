import azure.functions as func
import logging
import os
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="ingesta_raw", methods=["POST"])
def ingesta_raw(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Iniciando ingesta RAW")

    # 1. Leer JSON
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Body no es un JSON válido",
            status_code=400
        )

    # 2. Configuración Storage
    account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    container_name = os.getenv("STORAGE_CONTAINER_NAME")
    account_key = os.getenv("STORAGE_ACCOUNT_KEY")

    if not all([account_name, container_name, account_key]):
        return func.HttpResponse(
            "Variables de entorno de Storage no configuradas",
            status_code=500
        )

    # 3. Cliente Blob
    blob_service_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key
    )

    container_client = blob_service_client.get_container_client(container_name)

    # 4. Estructura RAW por fecha
    now = datetime.utcnow()
    path = (
        f"ingesta_ccma/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"ingest_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    # 5. Subir archivo
    blob_client = container_client.get_blob_client(path)
    blob_client.upload_blob(
        json.dumps(data, ensure_ascii=False),
        overwrite=True
    )

    logging.info(f"Archivo almacenado en RAW: {path}")

    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "path": path
        }),
        mimetype="application/json",
        status_code=200
    )
