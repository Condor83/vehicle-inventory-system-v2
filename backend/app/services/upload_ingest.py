from __future__ import annotations

import io
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd
from sqlalchemy import select

from backend.app.db import models
from backend.app.db.session import session_scope
from backend.app.services.ingest import upsert_observations_and_listings

SOURCE_RANK_UPLOAD = 80


@dataclass
class UploadProcessingSummary:
    upload_id: int
    filename: str
    rows_ingested: int
    rows_updated: int
    errors: List[Dict[str, Any]]
    row_errors: List[Dict[str, Any]]


def ingest_vehicle_locator_upload(filename: str, content: bytes) -> Dict[str, Any]:
    if not content:
        raise ValueError("Uploaded file is empty")

    upload_id = _create_upload_stub(filename)
    try:
        summary = _process_vehicle_locator(upload_id, filename, content)
    except Exception as exc:  # pragma: no cover - defensive logging
        _mark_upload_failed(upload_id, str(exc))
        raise
    else:
        _mark_upload_completed(summary)
        return {
            "upload_id": summary.upload_id,
            "filename": summary.filename,
            "rows_ingested": summary.rows_ingested,
            "rows_updated": summary.rows_updated,
            "errors": summary.errors,
            "row_errors": summary.row_errors,
            "status": "completed",
        }


def _create_upload_stub(filename: str) -> int:
    with session_scope() as session:
        upload = models.Upload(
            filename=filename,
            status="processing",
            rows_ingested=0,
            rows_updated=0,
            errors=[],
            row_errors=[],
        )
        session.add(upload)
        session.flush()
        return upload.id


def _mark_upload_failed(upload_id: int, message: str) -> None:
    with session_scope() as session:
        upload = session.get(models.Upload, upload_id)
        if not upload:
            return
        upload.status = "failed"
        upload.errors = [{"message": message}]
        upload.row_errors = []
        upload.processed_at = datetime.now(timezone.utc)


def _mark_upload_completed(summary: UploadProcessingSummary) -> None:
    with session_scope() as session:
        upload = session.get(models.Upload, summary.upload_id)
        if not upload:
            return
        upload.status = "completed"
        upload.rows_ingested = summary.rows_ingested
        upload.rows_updated = summary.rows_updated
        upload.errors = summary.errors
        upload.row_errors = summary.row_errors
        upload.processed_at = datetime.now(timezone.utc)


def _process_vehicle_locator(upload_id: int, filename: str, content: bytes) -> UploadProcessingSummary:
    dataframe = _load_vehicle_locator(filename, content)
    if dataframe.empty:
        return UploadProcessingSummary(
            upload_id=upload_id,
            filename=filename,
            rows_ingested=0,
            rows_updated=0,
            errors=[],
            row_errors=[],
        )

    dealer_code_map = _load_dealer_code_map()
    now = datetime.now(timezone.utc)
    job_uuid = uuid4()

    processed_rows: List[Dict[str, Any]] = []
    row_errors: List[Dict[str, Any]] = []
    dealer_updates: Dict[int, Dict[str, Any]] = {}
    seen_keys: Dict[Tuple[str, int], Tuple[int, int]] = {}

    records = dataframe.to_dict(orient="records")
    for idx, record in enumerate(records):
        row_number = idx + 2  # account for header row
        original_row = _sanitize_original_row(dataframe.columns, record)
        lookup = {str(k).lower(): v for k, v in original_row.items()}

        vin = _normalize_vin(lookup.get("vin"))
        if not vin:
            row_errors.append({"row": row_number, "vin": None, "message": "Missing VIN"})
            continue

        dealer_code_raw = lookup.get("dealer code")
        dealer_code = _normalize_dealer_code(dealer_code_raw)
        if not dealer_code:
            row_errors.append({"row": row_number, "vin": vin, "message": "Missing dealer code"})
            continue

        dealer_id = dealer_code_map.get(dealer_code)
        if dealer_id is None:
            row_errors.append(
                {"row": row_number, "vin": vin, "message": f"Unknown dealer code {dealer_code_raw}"}
            )
            continue

        msrp = _parse_decimal(lookup.get("total srp") or lookup.get("msrp"))
        invoice_price = _parse_decimal(lookup.get("invoice"))
        year = _parse_int(lookup.get("yr.") or lookup.get("year"))

        status = _derive_status(lookup)
        stock_number = lookup.get("stock") or lookup.get("stock #") or lookup.get("stock number")

        vehicle_features = {"vehicle_locator": {**original_row, "upload_id": upload_id}}
        vehicle_data = {
            "make": "Toyota",
            "model": lookup.get("model name") or lookup.get("model"),
            "year": year,
            "trim": lookup.get("trim"),
            "drivetrain": lookup.get("drivetrain"),
            "transmission": lookup.get("transmission"),
            "exterior_color": lookup.get("ext.") or lookup.get("exterior"),
            "interior_color": lookup.get("int.") or lookup.get("interior"),
            "msrp": msrp,
            "invoice_price": invoice_price,
            "features": vehicle_features,
        }

        payload = {
            "upload_id": upload_id,
            "filename": filename,
            "row_index": row_number,
            "vehicle_locator": original_row,
            "dealer_code": dealer_code,
        }

        prepared_row = {
            "dealer_id": dealer_id,
            "vin": vin,
            "advertised_price": None,
            "msrp": msrp,
            "status": status,
            "observed_at": now,
            "job_id": str(job_uuid),
            "source": "upload",
            "source_rank": SOURCE_RANK_UPLOAD,
            "payload": payload,
            "vehicle": vehicle_data,
            "stock_number": stock_number,
        }

        dedupe_key = (vin, dealer_id)
        existing_entry = seen_keys.get(dedupe_key)
        if existing_entry:
            previous_row, existing_index = existing_entry
            row_errors.append(
                {
                    "row": row_number,
                    "vin": vin,
                    "message": f"Duplicate entry for dealer {dealer_code}; replacing row {previous_row}",
                }
            )
            processed_rows[existing_index] = prepared_row
            seen_keys[dedupe_key] = (row_number, existing_index)
        else:
            list_index = len(processed_rows)
            processed_rows.append(prepared_row)
            seen_keys[dedupe_key] = (row_number, list_index)

        dealer_update_entry = dealer_updates.setdefault(dealer_id, {})
        region_value = lookup.get("region")
        if region_value:
            dealer_update_entry["region"] = region_value
        district_value = _parse_int(lookup.get("district"))
        if district_value is not None:
            dealer_update_entry["district_code"] = district_value
        phone_value = lookup.get("dealer phone")
        if phone_value:
            dealer_update_entry["phone"] = phone_value

    vin_list = [row["vin"] for row in processed_rows]
    existing_vins = _fetch_existing_vins(vin_list) if vin_list else set()
    rows_ingested = len(processed_rows)
    rows_updated = sum(1 for vin in vin_list if vin in existing_vins)

    if processed_rows:
        upsert_observations_and_listings(processed_rows, source="upload")
        _apply_dealer_enrichment(dealer_updates)

    errors_summary: List[Dict[str, Any]] = []
    if row_errors:
        errors_summary.append({"type": "row_errors", "count": len(row_errors)})

    return UploadProcessingSummary(
        upload_id=upload_id,
        filename=filename,
        rows_ingested=rows_ingested,
        rows_updated=rows_updated,
        errors=errors_summary,
        row_errors=row_errors,
    )


def _load_vehicle_locator(filename: str, content: bytes) -> pd.DataFrame:
    buffer = io.BytesIO(content)
    try:
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(buffer)
        else:
            df = pd.read_excel(buffer)
    except Exception as exc:
        raise ValueError(f"Unable to read spreadsheet: {exc}") from exc

    df.columns = [str(col).strip() for col in df.columns]
    df = df.dropna(how="all")
    return df


def _sanitize_original_row(columns: List[str], record: Dict[str, Any]) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = {}
    for column in columns:
        value = record.get(column)
        if _is_null(value):
            sanitized[column] = None
            continue
        if isinstance(value, pd.Timestamp):
            sanitized[column] = value.date().isoformat()
        elif isinstance(value, datetime):
            sanitized[column] = value.isoformat()
        elif isinstance(value, (float, int)) and not isinstance(value, bool):
            if isinstance(value, float) and value.is_integer():
                sanitized[column] = int(value)
            else:
                sanitized[column] = value
        else:
            sanitized[column] = str(value).strip()
    return sanitized


def _normalize_vin(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    vin = str(value).strip().upper()
    return vin if len(vin) == 17 else None


def _normalize_dealer_code(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        if isinstance(value, float) and (math.isnan(value) or value == 0):
            return None
        return str(int(value))
    return str(value).strip() or None


def _parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
        return Decimal(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _derive_status(lookup: Dict[str, Any]) -> str:
    presold = str(lookup.get("presold")).strip().lower() if lookup.get("presold") is not None else ""
    reserved = str(lookup.get("res.")).strip().lower() if lookup.get("res.") is not None else ""
    if presold in {"true", "yes", "y", "1"} or reserved in {"y", "yes"}:
        return "pending"
    return "in_transit"


def _load_dealer_code_map() -> Dict[str, int]:
    with session_scope() as session:
        rows = session.execute(select(models.Dealer.id, models.Dealer.code)).all()
        return {str(code).strip(): dealer_id for dealer_id, code in rows if code}


def _fetch_existing_vins(vins: List[str]) -> set[str]:
    if not vins:
        return set()
    with session_scope() as session:
        rows = session.execute(
            select(models.Vehicle.vin).where(models.Vehicle.vin.in_(vins))
        )
        return {vin for (vin,) in rows}


def _apply_dealer_enrichment(updates: Dict[int, Dict[str, Any]]) -> None:
    if not updates:
        return
    with session_scope() as session:
        for dealer_id, payload in updates.items():
            dealer = session.get(models.Dealer, dealer_id)
            if not dealer:
                continue
            phone = payload.get("phone")
            if phone and phone != dealer.phone:
                dealer.phone = phone
            region = payload.get("region")
            if region and region != dealer.region:
                dealer.region = region
            district = payload.get("district_code")
            if district and district != dealer.district_code:
                dealer.district_code = str(district)


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, pd.Series):
        return value.isna().all()
    try:
        return bool(pd.isna(value))
    except Exception:  # pragma: no cover - fallback for complex objects
        return False
