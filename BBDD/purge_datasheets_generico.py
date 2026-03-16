#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Borra y purga de la base de datos de ckan todos los datasheets de la organizacion indicada.

Requisitos:
    pip install ckanapi

Uso:
    python purge_datasheets_generico.py \
        --ckan-url https://tu-ckan.ejemplo \
        --api-key TU_API_KEY \
        --owner-org universidad-politecnica-de-madrid

Comportamiento:
- Primero borra todos los datasheets de la organizacion indicada, marcandolos como deleted (no visible).
- Seguidamente purga de la base de datos de ckan todos los datasheets de la organizacion indicada.
"""

import sys
import argparse
import logging
import ckanapi
from ckanapi.errors import CKANAPIError

ROWS = 1000


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
        logging.FileHandler("ckan_datasheet_purge.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    )
    return logging.getLogger()


def get_datasets(ckan, org):
    start = 0

    while True:

        result = ckan.action.package_search(
            fq=f"organization:{org}",
            rows=ROWS,
            start=start
        )

        datasets = result["results"]

        if not datasets:
            break

        for d in datasets:
            yield d["name"]

        start += ROWS


def confirm_execution(org, dry_run):
    if dry_run:
        print("DRY RUN MODE - No datasets will be deleted\n")
        return

    confirm = input(
        f"⚠️  This will DELETE and PURGE all datasets in organization '{org}'. Continue? (yes/no): "
    )

    if confirm.lower() != "yes":
        print("Operation cancelled.")
        exit()


def main():
    parser = argparse.ArgumentParser(description="Purge datasets from CKAN organization")

    parser.add_argument("--ckan-url", required=True, help="URL base de CKAN")
    parser.add_argument("--api-key", required=True, help="API token de CKAN")
    parser.add_argument("--owner-org", required=True, help="ID o nombre de la organización CKAN")
    parser.add_argument("--dry-run", action="store_true", help="Simula la operación sin borrar datasets")

    args = parser.parse_args()
    logger = setup_logger()
    ckan = ckanapi.RemoteCKAN(args.ckan_url, apikey=args.api_key)
    confirm_execution(args.owner_org, args.dry_run)
    count = 0

    for dataset_id in get_datasets(ckan, args.owner_org):
        count += 1
        print(f"[{count}] Processing dataset → {dataset_id}")

        try:
            if args.dry_run:
                logger.info(f"DRY RUN → {dataset_id}")
                continue

            logger.info(f"DELETE dataset → {dataset_id}")
            ckan.action.package_delete(id=dataset_id)

            logger.info(f"PURGE dataset → {dataset_id}")
            ckan.action.dataset_purge(id=dataset_id)

            logger.info(f"SUCCESS → {dataset_id}")

        except CKANAPIError as e:
            logger.error(f"CKAN ERROR {dataset_id} → {str(e)}")
            print(f"CKAN error with dataset {dataset_id}")

        except Exception as e:
            logger.error(f"GENERAL ERROR {dataset_id} → {str(e)}")
            print(f"Unexpected error with dataset {dataset_id}")

    print(f"\nFinished. Total processed: {count}")


if __name__ == "__main__":
    main()