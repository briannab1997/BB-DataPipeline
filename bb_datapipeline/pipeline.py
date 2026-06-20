"""Small but practical CSV ETL pipeline used by the portfolio demo."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


REQUIRED_FIELDS = [
    "order_id",
    "customer_id",
    "order_date",
    "ship_date",
    "region",
    "category",
    "quantity",
    "unit_price",
    "status",
]

VALID_STATUSES = {
    "delivered": "Delivered",
    "shipped": "Shipped",
    "processing": "Processing",
    "cancelled": "Cancelled",
    "returned": "Returned",
}

REGION_ALIASES = {
    "northeast": "Northeast",
    "north east": "Northeast",
    "ne": "Northeast",
    "southeast": "Southeast",
    "south east": "Southeast",
    "se": "Southeast",
    "midwest": "Midwest",
    "west": "West",
}


@dataclass(frozen=True)
class PipelineIssue:
    row_number: int
    order_id: str
    field: str
    severity: str
    message: str


@dataclass(frozen=True)
class PipelineResult:
    cleaned_rows: list[dict[str, str]]
    issues: list[PipelineIssue]
    summary: dict[str, object]


def run_pipeline(input_path: Path) -> PipelineResult:
    rows = _read_csv(input_path)
    seen_order_ids: set[str] = set()
    cleaned_rows: list[dict[str, str]] = []
    issues: list[PipelineIssue] = []

    for index, row in enumerate(rows, start=2):
        order_id = row.get("order_id", "").strip()
        row_issues: list[PipelineIssue] = []

        for field in REQUIRED_FIELDS:
            if not row.get(field, "").strip():
                row_issues.append(_issue(index, order_id, field, "high", f"{field} is required."))

        if order_id:
            if order_id in seen_order_ids:
                row_issues.append(_issue(index, order_id, "order_id", "high", "Duplicate order ID found."))
            seen_order_ids.add(order_id)

        order_date = _parse_date(row.get("order_date", ""))
        ship_date = _parse_date(row.get("ship_date", ""))
        if row.get("order_date") and order_date is None:
            row_issues.append(_issue(index, order_id, "order_date", "high", "Order date is not a valid YYYY-MM-DD date."))
        if row.get("ship_date") and ship_date is None:
            row_issues.append(_issue(index, order_id, "ship_date", "high", "Ship date is not a valid YYYY-MM-DD date."))
        if order_date and ship_date and ship_date < order_date:
            row_issues.append(_issue(index, order_id, "ship_date", "high", "Ship date occurs before order date."))

        quantity = _parse_decimal(row.get("quantity", ""))
        unit_price = _parse_decimal(row.get("unit_price", ""))
        if row.get("quantity") and (quantity is None or quantity <= 0):
            row_issues.append(_issue(index, order_id, "quantity", "high", "Quantity must be greater than zero."))
        if row.get("unit_price") and (unit_price is None or unit_price <= 0):
            row_issues.append(_issue(index, order_id, "unit_price", "high", "Unit price must be greater than zero."))

        status = _clean_status(row.get("status", ""))
        if row.get("status") and status is None:
            row_issues.append(_issue(index, order_id, "status", "high", "Status is outside the expected values."))

        region = _clean_region(row.get("region", ""))
        if row.get("region") and region is None:
            row_issues.append(_issue(index, order_id, "region", "medium", "Region could not be standardized."))

        issues.extend(row_issues)

        if not any(issue.severity == "high" for issue in row_issues):
            cleaned_rows.append(
                _transform_row(
                    row=row,
                    order_date=order_date,
                    ship_date=ship_date,
                    quantity=quantity,
                    unit_price=unit_price,
                    status=status,
                    region=region,
                )
            )

    return PipelineResult(
        cleaned_rows=cleaned_rows,
        issues=issues,
        summary=_build_summary(rows, cleaned_rows, issues),
    )


def write_reports(result: PipelineResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_clean_orders(result.cleaned_rows, output_dir / "clean_orders.csv")
    _write_issues(result.issues, output_dir / "pipeline_issues.csv")
    (output_dir / "pipeline_summary.json").write_text(json.dumps(result.summary, indent=2), encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def _issue(row_number: int, order_id: str, field: str, severity: str, message: str) -> PipelineIssue:
    return PipelineIssue(row_number=row_number, order_id=order_id or "UNKNOWN", field=field, severity=severity, message=message)


def _parse_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_decimal(value: str) -> Decimal | None:
    value = value.strip().replace("$", "")
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _clean_status(value: str) -> str | None:
    return VALID_STATUSES.get(value.strip().lower())


def _clean_region(value: str) -> str | None:
    return REGION_ALIASES.get(value.strip().lower())


def _transform_row(
    row: dict[str, str],
    order_date: date | None,
    ship_date: date | None,
    quantity: Decimal | None,
    unit_price: Decimal | None,
    status: str | None,
    region: str | None,
) -> dict[str, str]:
    quantity = quantity or Decimal("0")
    unit_price = unit_price or Decimal("0")
    revenue = quantity * unit_price
    fulfillment_days = (ship_date - order_date).days if order_date and ship_date else ""

    return {
        "order_id": row.get("order_id", "").strip(),
        "customer_id": row.get("customer_id", "").strip().upper(),
        "order_date": order_date.isoformat() if order_date else "",
        "ship_date": ship_date.isoformat() if ship_date else "",
        "fulfillment_days": str(fulfillment_days),
        "region": region or row.get("region", "").strip(),
        "category": row.get("category", "").strip().title(),
        "quantity": str(int(quantity)),
        "unit_price": f"{unit_price:.2f}",
        "revenue": f"{revenue:.2f}",
        "status": status or row.get("status", "").strip().title(),
        "priority_lane": "Yes" if revenue >= Decimal("500") or status == "Processing" else "No",
    }


def _build_summary(
    raw_rows: list[dict[str, str]],
    cleaned_rows: list[dict[str, str]],
    issues: list[PipelineIssue],
) -> dict[str, object]:
    total_revenue = sum(Decimal(row["revenue"]) for row in cleaned_rows) if cleaned_rows else Decimal("0")
    issue_fields = _count_by((issue.field for issue in issues))
    severity_counts = _count_by((issue.severity for issue in issues))
    revenue_by_region: dict[str, Decimal] = {}
    status_counts: dict[str, int] = {}

    for row in cleaned_rows:
        revenue_by_region[row["region"]] = revenue_by_region.get(row["region"], Decimal("0")) + Decimal(row["revenue"])
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1

    issue_penalty = len(issues) * 4 + severity_counts.get("high", 0) * 5
    pipeline_score = max(0, min(100, 100 - issue_penalty))

    return {
        "rows_processed": len(raw_rows),
        "clean_records": len(cleaned_rows),
        "rejected_records": len(raw_rows) - len(cleaned_rows),
        "issues_found": len(issues),
        "high_severity_issues": severity_counts.get("high", 0),
        "pipeline_score": pipeline_score,
        "total_revenue": f"{total_revenue:.2f}",
        "revenue_by_region": {key: f"{value:.2f}" for key, value in sorted(revenue_by_region.items())},
        "status_counts": dict(sorted(status_counts.items())),
        "top_issue_fields": dict(sorted(issue_fields.items(), key=lambda item: item[1], reverse=True)[:5]),
    }


def _count_by(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _write_clean_orders(rows: list[dict[str, str]], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_issues(issues: list[PipelineIssue], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["row_number", "order_id", "field", "severity", "message"])
        writer.writeheader()
        for issue in issues:
            writer.writerow(
                {
                    "row_number": issue.row_number,
                    "order_id": issue.order_id,
                    "field": issue.field,
                    "severity": issue.severity,
                    "message": issue.message,
                }
            )
