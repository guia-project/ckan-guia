import requests
import os
import logging
from dotenv import load_dotenv

load_dotenv()

CKAN_URL = os.getenv("CKAN_URL_ACTION")
API_KEY = os.getenv("CKAN_API_KEY")
ORG = os.getenv("CKAN_ORG")

headers = {"Authorization": API_KEY}

ROWS = 1000


# configurar logging
logging.basicConfig(
    filename="ckan_datasheet_purge.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger()


def get_datasets():
    start = 0

    while True:
        r = requests.get(
            CKAN_URL + "package_search",
            headers=headers,
            params={
                "fq": f"organization:{ORG}",
                "rows": ROWS,
                "start": start
            }
        )

        result = r.json()["result"]
        datasets = result["results"]

        if not datasets:
            break

        for d in datasets:
            yield d["name"]

        start += ROWS


datasets = list(get_datasets())

logger.info(f"Datasets encontrados en la organización '{ORG}': {len(datasets)}")


for dataset_id in datasets:

    try:

        logger.info(f"DELETE dataset → {dataset_id}")

        requests.post(
            CKAN_URL + "package_delete",
            headers=headers,
            json={"id": dataset_id}
        )

        logger.info(f"PURGE dataset → {dataset_id}")

        requests.post(
            CKAN_URL + "dataset_purge",
            headers=headers,
            json={"id": dataset_id}
        )

        logger.info(f"SUCCESS → {dataset_id}")

    except Exception as e:
        logger.error(f"ERROR con dataset {dataset_id} → {str(e)}")