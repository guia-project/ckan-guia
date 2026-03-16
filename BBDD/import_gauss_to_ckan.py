#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Importa filas de un CSV a CKAN registrando la URL de un PDF como recurso remoto.

Requisitos:
    pip install ckanapi

Uso:
    python import_gauss_to_ckan.py \
        --csv gauss_index_old.csv \
        --ckan-url https://tu-ckan.ejemplo \
        --api-key TU_API_KEY \
        --owner-org universidad-politecnica-de-madrid

Comportamiento:
- Crea un dataset por fila del CSV.
- Si el dataset ya existe, lo actualiza parcialmente con package_patch.
- Si el recurso PDF ya existe por URL, no lo duplica.
"""

import argparse
import csv
import re
import sys
import time
import unicodedata
import logging
from typing import Dict, Any, List, Optional

from ckanapi import RemoteCKAN
from ckanapi.errors import CKANAPIError, NotFound


# ----------------------------
# LOGGING CONFIG
# ----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("gauss_import.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ----------------------------
# CONTADORES
# ----------------------------

stats = {
    "created": 0,
    "updated": 0,
    "resources": 0,
    "skipped": 0,
    "errors": 0,
}


# ----------------------------
# UTILIDADES
# ----------------------------

def slugify(text: str, max_length: int = 100) -> str:
    """
    Convierte texto a slug CKAN-compatible:
    - minúsculas
    - ascii
    - solo [a-z0-9_-]
    """
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9_-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-_")
    if len(text) > max_length:
        text = text[:max_length].rstrip("-_")
    if len(text) < 2:
        text = f"ds-{text}" if text else "ds-empty"
    return text


def dataset_name_from_row(row: Dict[str, str]) -> str:
    raw = f"{row.get('academic_year','')}-{row.get('semester','')}-{row.get('study_plan_code','')}-{row.get('subject_code','')}-{row.get('subject_name','')}"
    return slugify(raw, max_length=100)


def dataset_title_from_row(row: Dict[str, str]) -> str:
    return f"{row.get('subject_name','').strip()} [{row.get('subject_code','').strip()}] - {row.get('academic_year','').strip()} {row.get('semester','').strip()}"


def build_notes(row: Dict[str, str]) -> str:
    lines = [
        f"Guía docente de la asignatura {row.get('subject_name','').strip()}",
        "",
        f"Año académico: {row.get('academic_year','').strip()}",
        f"Semestre: {row.get('semester','').strip()}",
        f"Plan: {row.get('study_plan_name','').strip()}",
        f"Código de plan: {row.get('study_plan_code','').strip()}",
        f"Tipo de plan: {row.get('study_plan_type','').strip()}",
        f"Tipo de asignatura: {row.get('subject_type','').strip()}",
        f"Código de asignatura: {row.get('subject_code','').strip()}",
    ]
    return "\n".join(lines)


def build_extras(row: Dict[str, str]) -> List[Dict[str, str]]:
    fields = [
        "academic_year","semester","study_plan_type","study_plan_code",
        "study_plan_name","subject_type","subject_code","subject_name",
        "info_description_url","info_professors_url",
        "info_prev_requirements_url","info_competences_and_results_url",
        "info_syllabus_url","info_schedule_url","info_evaluation_url",
        "info_resources_url","info_other_url",
    ]

    extras = []
    for field in fields:
        value = row.get(field, "")
        if value:
            extras.append({"key": field, "value": value})

    return extras


def build_tags(row: Dict[str, str]) -> List[Dict[str, str]]:
    tags = []

    for value in [
        row.get("academic_year",""),
        row.get("semester",""),
        row.get("study_plan_code",""),
        row.get("subject_code",""),
    ]:
        tag = slugify(value, max_length=50)
        if tag:
            tags.append({"name": tag})

    return tags


def resource_already_exists(resources: List[Dict[str, Any]], pdf_url: str) -> bool:
    for resource in resources or []:
        if resource.get("url","").strip() == pdf_url.strip():
            return True
    return False


# ----------------------------
# CKAN OPERATIONS
# ----------------------------

def ensure_dataset(
    ckan: RemoteCKAN,
    row: Dict[str, str],
    owner_org: str,
    license_id: Optional[str] = None,
) -> Dict[str, Any]:

    dataset_name = dataset_name_from_row(row)
    title = dataset_title_from_row(row)
    notes = build_notes(row)
    extras = build_extras(row)
    tags = build_tags(row)

    payload = {
        "name": dataset_name,
        "title": title,
        "notes": notes,
        "owner_org": owner_org,
        "extras": extras,
        "tags": tags,
    }

    if license_id:
        payload["license_id"] = license_id

    try:

        existing = ckan.action.package_show(id=dataset_name)
        dataset_id = existing["id"]

        patch_payload = {
            "id": dataset_id,
            "title": title,
            "notes": notes,
            "extras": extras,
            "tags": tags,
        }

        if license_id:
            patch_payload["license_id"] = license_id

        updated = ckan.action.package_patch(**patch_payload)

        logger.info(f"Dataset actualizado: {dataset_name}")
        stats["updated"] += 1

        return updated

    except NotFound:

        created = ckan.action.package_create(**payload)

        logger.info(f"Dataset creado: {dataset_name}")
        stats["created"] += 1

        return created


def ensure_pdf_resource(
    ckan: RemoteCKAN,
    dataset: Dict[str, Any],
    row: Dict[str, str],
) -> Optional[Dict[str, Any]]:

    pdf_url = row.get("guide_pdf_url","").strip()

    if not pdf_url:
        logger.warning(f"Fila sin guide_pdf_url: {dataset.get('name')}")
        stats["skipped"] += 1
        return None

    current = ckan.action.package_show(id=dataset["id"])

    if resource_already_exists(current.get("resources",[]), pdf_url):
        logger.info(f"Recurso ya existe en {dataset.get('name')}")
        stats["skipped"] += 1
        return None

    resource = ckan.action.resource_create(
        package_id=dataset["id"],
        url=pdf_url,
        name=f"Guía docente PDF - {row.get('subject_name','').strip()}",
        description=f"PDF remoto {row.get('subject_name','')}",
        format="PDF",
        resource_type="file",
        mimetype="application/pdf",
    )

    logger.info(f"Recurso PDF creado en {dataset.get('name')}")
    stats["resources"] += 1

    return resource


# ----------------------------
# CSV PROCESSING
# ----------------------------

def process_csv(
    csv_path: str,
    ckan_url: str,
    api_key: str,
    owner_org: str,
    license_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> None:

    logger.info(f"Inicio import CSV: {csv_path}")

    ckan = RemoteCKAN(ckan_url, apikey=api_key, user_agent="gauss-importer/1.0")

    with open(csv_path, "r", encoding="utf-8", newline="") as f:

        reader = csv.DictReader(f)

        required_columns = {
            "academic_year","semester","study_plan_code",
            "subject_code","subject_name","guide_pdf_url",
        }

        missing = required_columns - set(reader.fieldnames or [])

        if missing:
            raise ValueError(f"Faltan columnas en CSV: {sorted(missing)}")

        for idx, row in enumerate(reader, start=1):
            if limit and idx > limit:
                break

            try:
                dataset = ensure_dataset(
                    ckan=ckan,
                    row=row,
                    owner_org=owner_org,
                    license_id=license_id,
                )
                ensure_pdf_resource(
                    ckan=ckan,
                    dataset=dataset,
                    row=row,
                )

            except CKANAPIError as e:
                logger.error(f"Fila {idx} ({row.get('subject_code')}): {e}")
                stats["errors"] += 1

            except Exception as e:
                logger.error(f"Fila {idx} ({row.get('subject_code')}): {e}")
                stats["errors"] += 1

    logger.info("Import terminado")


# ----------------------------
# ARGUMENTS
# ----------------------------

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Importa URLs de PDFs desde CSV a CKAN como recursos remotos."
    )
    parser.add_argument("--csv", required=True, help="Ruta al CSV")
    parser.add_argument("--ckan-url", required=True, help="URL base de CKAN")
    parser.add_argument("--api-key", required=True, help="API token de CKAN")
    parser.add_argument("--owner-org", required=True, help="ID o nombre de la organización CKAN")
    parser.add_argument("--license-id", default=None, help="license_id opcional de CKAN")
    parser.add_argument("--limit", type=int, default=None, help="Número máximo de filas a procesar")
    return parser.parse_args()


def main() -> None:

    args = parse_args()

    start = time.time()

    process_csv(
        csv_path=args.csv,
        ckan_url=args.ckan_url,
        api_key=args.api_key,
        owner_org=args.owner_org,
        license_id=args.license_id,
        limit=args.limit,
    )

    duration = round(time.time() - start, 2)

    logger.info("--------- IMPORT SUMMARY ---------")
    logger.info(f"Datasets created : {stats['created']}")
    logger.info(f"Datasets updated : {stats['updated']}")
    logger.info(f"Resources added  : {stats['resources']}")
    logger.info(f"Skipped          : {stats['skipped']}")
    logger.info(f"Errors           : {stats['errors']}")
    logger.info(f"Duration         : {duration}s")
    logger.info("----------------------------------")


if __name__ == "__main__":
    main()