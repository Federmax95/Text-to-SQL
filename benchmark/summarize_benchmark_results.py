import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = BASE_DIR / "results"


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sum_result_times(results: list[dict[str, Any]]) -> tuple[float, float]:
    baseline_total = 0.0
    pipeline_total = 0.0

    for item in results:
        baseline_total += _as_float(item.get("baseline_time_seconds",
                                    item.get("baseline_time", 0.0)))
        pipeline_total += _as_float(item.get("pipeline_time_seconds",
                                    item.get("pipeline_time", 0.0)))

    return round(baseline_total, 2), round(pipeline_total, 2)


def _load_summary(file_path: Path, results_dir: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    results = data.get("results") or []
    if not isinstance(results, list):
        results = []

    baseline_time_total, pipeline_time_total = _sum_result_times(results)
    total_time_total = round(baseline_time_total + pipeline_time_total, 2)

    total_tests = int(data.get("total_tests") or len(results) or 0)
    baseline_matched = int(data.get("baseline_matched") or 0)
    pipeline_matched = int(data.get("pipeline_matched") or 0)

    # Compute per-item discordant counts if available in results list
    baseline_only_correct = 0
    pipeline_only_correct = 0
    both_correct = 0
    per_item_flags = False
    for item in results:
        if isinstance(item, dict) and ("baseline_correct" in item or "pipeline_correct" in item):
            per_item_flags = True
            b = bool(item.get("baseline_correct") or False)
            p = bool(item.get("pipeline_correct") or False)
            if b and p:
                both_correct += 1
            if b and not p:
                baseline_only_correct += 1
            if p and not b:
                pipeline_only_correct += 1

    if not per_item_flags:
        baseline_only_correct = None
        pipeline_only_correct = None
        both_correct = None

    baseline_wrong = total_tests - baseline_matched
    pipeline_wrong = total_tests - pipeline_matched

    try:
        display_file = str(file_path.relative_to(
            results_dir)).replace("\\", "/")
    except ValueError:
        display_file = file_path.name

    return {
        "file": display_file,
        "timestamp": data.get("timestamp", ""),
        "model": data.get("model", ""),
        "method": data.get("method", ""),
        "total_tests": total_tests,
        "baseline_accuracy": _as_float(data.get("baseline_accuracy", 0.0)),
        "pipeline_accuracy": _as_float(data.get("pipeline_accuracy", 0.0)),
        "baseline_matched": baseline_matched,
        "pipeline_matched": pipeline_matched,
        "baseline_correct_total": baseline_matched,
        "pipeline_correct_total": pipeline_matched,
        "baseline_wrong": baseline_wrong,
        "pipeline_wrong": pipeline_wrong,
        "baseline_only_correct": baseline_only_correct,
        "pipeline_only_correct": pipeline_only_correct,
        "both_correct": both_correct,
        "baseline_time_total": baseline_time_total,
        "pipeline_time_total": pipeline_time_total,
        "total_time_total": total_time_total,
    }


def _iter_result_files(results_dir: Path) -> list[Path]:
    return sorted(
        [path for path in results_dir.rglob("*.json") if path.is_file()],
        key=lambda path: path.as_posix().lower(),
    )


def _format_seconds(value: float) -> str:
    return f"{value:.2f}"


def _format_markdown_table(rows: list[dict[str, Any]], totals: dict[str, Any]) -> str:
    headers = [
        "file",
        "model",
        "tests",
        "baseline_acc_%",
        "pipeline_acc_%",
        "baseline_time_s",
        "pipeline_time_s",
        "total_time_s",
    ]

    def row_to_cells(row: dict[str, Any]) -> list[str]:
        return [
            row["file"],
            row["model"],
            str(row["total_tests"]),
            f'{row["baseline_accuracy"]:.2f}',
            f'{row["pipeline_accuracy"]:.2f}',
            _format_seconds(row["baseline_time_total"]),
            _format_seconds(row["pipeline_time_total"]),
            _format_seconds(row["total_time_total"]),
        ]

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in rows:
        lines.append("| " + " | ".join(row_to_cells(row)) + " |")

    lines.append(
        "| TOTAL | - | {tests} | - | - | {baseline} | {pipeline} | {total} |".format(
            tests=totals["total_tests"],
            baseline=_format_seconds(totals["baseline_time_total"]),
            pipeline=_format_seconds(totals["pipeline_time_total"]),
            total=_format_seconds(totals["total_time_total"]),
        )
    )
    return "\n".join(lines)


def _write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    fieldnames = [
        "file",
        "timestamp",
        "model",
        "method",
        "total_tests",
        "baseline_accuracy",
        "pipeline_accuracy",
        "baseline_matched",
        "baseline_wrong",
        "pipeline_matched",
        "pipeline_wrong",
        "baseline_only_correct",
        "pipeline_only_correct",
        "baseline_time_total",
        "pipeline_time_total",
        "total_time_total",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _write_json(rows: list[dict[str, Any]], totals: dict[str, Any], output_path: Path) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(),
        "totals": totals,
        "rows": rows,
    }
    output_path.write_text(json.dumps(
        payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate benchmark result JSON files into a comparison table"
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Directory to scan recursively for benchmark result JSON files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output file. If omitted, the table is printed to stdout",
    )
    parser.add_argument(
        "--format",
        choices=("md", "csv", "json", "pdf"),
        default="md",
        help="Output format",
    )
    args = parser.parse_args()

    if not args.results_dir.exists():
        raise SystemExit(f"Cartella risultati non trovata: {args.results_dir}")

    rows = []
    for file_path in _iter_result_files(args.results_dir):
        summary = _load_summary(file_path, args.results_dir)
        if summary is not None:
            rows.append(summary)

    rows.sort(key=lambda row: (str(row["model"]).lower(), str(
        row["timestamp"]).lower(), str(row["file"]).lower()))

    totals = {
        "total_tests": sum(int(row["total_tests"]) for row in rows),
        "baseline_time_total": round(sum(float(row["baseline_time_total"]) for row in rows), 2),
        "pipeline_time_total": round(sum(float(row["pipeline_time_total"]) for row in rows), 2),
    }
    totals["total_time_total"] = round(
        totals["baseline_time_total"] + totals["pipeline_time_total"], 2)
    # Additional totals for wrong and discordant counts
    totals["baseline_matched_total"] = sum(
        int(row.get("baseline_matched") or 0) for row in rows)
    totals["pipeline_matched_total"] = sum(
        int(row.get("pipeline_matched") or 0) for row in rows)
    totals["baseline_wrong_total"] = sum(int(
        (row.get("total_tests") or 0) - (row.get("baseline_matched") or 0)) for row in rows)
    totals["pipeline_wrong_total"] = sum(int(
        (row.get("total_tests") or 0) - (row.get("pipeline_matched") or 0)) for row in rows)
    # For discordant totals, treat None as 0
    totals["baseline_only_correct_total"] = sum(
        int(row.get("baseline_only_correct") or 0) for row in rows)
    totals["pipeline_only_correct_total"] = sum(
        int(row.get("pipeline_only_correct") or 0) for row in rows)

    if args.format == "md":
        content = _format_markdown_table(rows, totals)
        if args.output:
            args.output.write_text(content + "\n", encoding="utf-8")
        else:
            print(content)
        return

    if args.format == "pdf":
        # Default output PDF path if not provided
        if not args.output:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.output = args.results_dir / f"benchmark_summary_{ts}.pdf"

        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import letter, A4
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        except Exception:
            raise SystemExit(
                "Per generare PDF installa reportlab: pip install reportlab")

        pdf_path = args.output
        doc = SimpleDocTemplate(str(pdf_path), pagesize=A4, rightMargin=24,
                                leftMargin=24, topMargin=24, bottomMargin=24)

        # Build table data (omit file name column for PDF)
        page_width, page_height = A4
        left_margin = doc.leftMargin
        right_margin = doc.rightMargin
        usable_width = page_width - left_margin - right_margin

        styles = getSampleStyleSheet()
        elems = []
        elems.append(Paragraph("Benchmark summary", styles["Title"]))
        elems.append(Spacer(1, 12))

        header = ["model", "tests", "baseline accuracy (%)", "pipeline accuracy (%)",
                  "baseline time (s)", "pipeline time (s)", "total time (s)"]
        data_table = [header]
        for r in rows:
            data_table.append([
                r.get("model", ""),
                str(r.get("total_tests", "")),
                f'{r.get("baseline_accuracy", 0):.2f}',
                f'{r.get("pipeline_accuracy", 0):.2f}',
                _format_seconds(r.get("baseline_time_total", 0.0)),
                _format_seconds(r.get("pipeline_time_total", 0.0)),
                _format_seconds(r.get("total_time_total", 0.0)),
            ])

        rel = [0.25, 0.08, 0.13, 0.13, 0.13, 0.13, 0.15]
        col_widths = [usable_width * r for r in rel]

        header_para = [Paragraph(h, styles["BodyText"]) for h in header]
        wrapped_data = [header_para]
        for row in data_table[1:]:
            model_para = Paragraph(str(row[0]), styles["BodyText"]) if row[0] else Paragraph(
                "", styles["BodyText"])
            new_row = [model_para] + \
                [Paragraph(str(c), styles["BodyText"]) for c in row[1:]]
            wrapped_data.append(new_row)

        table1 = Table(wrapped_data, colWidths=col_widths, repeatRows=1)
        style1 = TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f0f0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ])
        table1.setStyle(style1)
        elems.append(table1)
        elems.append(Spacer(1, 12))

        # SECOND TABLE: Summary per LLM with wrong/discordant counts
        # elems.append(Paragraph("Riepilogo per LLM", styles["Heading2"]))
        # elems.append(Spacer(1, 6))

        # Group rows by model name
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            model = str(r.get("model") or "UNKNOWN")
            grouped.setdefault(model, []).append(r)

        summary_header = [
            "model",
            "baseline wrong",
            "baseline correct",
            "pipeline wrong",
            "pipeline correct",
            "both correct",
            "pipeline wrong<br/>baseline correct",
            "pipeline correct<br/>baseline wrong",
        ]
        summary_data = [summary_header]

        for model_name in sorted(grouped.keys()):
            model_rows = grouped[model_name]
            baseline_wrong = sum(int((r.get("total_tests") or 0) -
                                 (r.get("baseline_matched") or 0)) for r in model_rows)
            baseline_correct = sum(int(r.get("baseline_correct_total") or 0)
                                   for r in model_rows)
            pipeline_wrong = sum(int((r.get("total_tests") or 0) -
                                 (r.get("pipeline_matched") or 0)) for r in model_rows)
            pipeline_correct = sum(int(r.get("pipeline_correct_total") or 0)
                                   for r in model_rows)
            both_correct = sum(int(r.get("both_correct") or 0)
                               for r in model_rows)
            baseline_only_correct = sum(
                int(r.get("baseline_only_correct") or 0) for r in model_rows)
            pipeline_only_correct = sum(
                int(r.get("pipeline_only_correct") or 0) for r in model_rows)

            summary_data.append([
                model_name,
                
                str(baseline_wrong),
                str(baseline_correct),
                str(pipeline_wrong),
                str(pipeline_correct),
                str(both_correct),
                str(baseline_only_correct),
                str(pipeline_only_correct),
            ])

        

        rel_summary = [0.20, 0.10, 0.10, 0.10, 0.10, 0.10, 0.15, 0.15]
        col_widths_summary = [usable_width * r for r in rel_summary]

        header_para_summary = [
            Paragraph(h, styles["BodyText"]) for h in summary_header]
        wrapped_data_summary = [header_para_summary]
        for row in summary_data[1:]:
            model_para = Paragraph(str(row[0]), styles["BodyText"]) if row[0] else Paragraph(
                "", styles["BodyText"])
            new_row = [model_para] + \
                [Paragraph(str(c), styles["BodyText"]) for c in row[1:]]
            wrapped_data_summary.append(new_row)

        table2 = Table(wrapped_data_summary,
                       colWidths=col_widths_summary, repeatRows=1)
        style2 = TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f0f0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("ALIGN", (1, 0), (-1, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ])
        table2.setStyle(style2)
        elems.append(table2)

        doc.build(elems)

        print(f"PDF generato: {pdf_path}")
        return

    if not args.output:
        raise SystemExit("Per i formati csv/json devi specificare --output")

    if args.format == "csv":
        _write_csv(rows, args.output)
        return

    _write_json(rows, totals, args.output)


if __name__ == "__main__":
    main()
