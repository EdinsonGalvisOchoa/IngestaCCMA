import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime
from azure.storage.blob import BlobServiceClient


# CONFIGURACIÓN

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = "raw"
BASE_PATH = "ingesta_ccma"


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ENDPOINT

@app.route(route="ingesta_raw", methods=["POST"])
def ingesta_raw(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Iniciando ingesta RAW")

    
    # 1. Leer JSON
  
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "El body debe ser un JSON válido",
            status_code=400
        )


    # 2. Validar campos obligatorios
 
    required_fields = ["nit", "empresa", "ciiu"]
    missing = [f for f in required_fields if f not in body]

    if missing:
        return func.HttpResponse(
            f"Faltan campos obligatorios: {missing}",
            status_code=400
        )


    # 3. Construir payload RAW (data + metadata)
   
    now = datetime.utcnow()

    payload = {
        "metadata": {
            "ingest_id": str(uuid.uuid4()),
            "ingest_timestamp": now.isoformat(),
            "source": "api-ingesta-ccma"
        },
        "data": body
    }


    # 4. Construir path particionado (year/month/day)
   
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    filename = f"ingest_{now.strftime('%Y%m%d_%H%M%S')}.json"
    blob_path = (
        f"{BASE_PATH}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{filename}"
    )


    # 5. Conexión a ADLS Gen2

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
        logging.error(f"Error escribiendo en ADLS: {str(e)}")
        return func.HttpResponse(
            "Error escribiendo en almacenamiento",
            status_code=500
        )


    # 6. Respuesta Ok
   
    return func.HttpResponse(
        json.dumps({
            "status": "ok",
            "message": "Archivo almacenado en RAW",
            "path": blob_path
        }),
        mimetype="application/json",
        status_code=200
    )
