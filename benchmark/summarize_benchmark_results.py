import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any
import os
from collections import defaultdict
import math
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

    # Average times per test
    baseline_time_avg = round(baseline_time_total /
                              total_tests, 2) if total_tests else 0.0
    pipeline_time_avg = round(pipeline_time_total /
                              total_tests, 2) if total_tests else 0.0

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
        "baseline_time_avg": baseline_time_avg,
        "pipeline_time_total": pipeline_time_total,
        "pipeline_time_avg": pipeline_time_avg,
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
        "baseline_avg_s",
        "pipeline_time_s",
        "pipeline_avg_s",
    ]

    def row_to_cells(row: dict[str, Any]) -> list[str]:
        return [
            row["file"],
            row["model"],
            str(row["total_tests"]),
            f'{row["baseline_accuracy"]:.2f}',
            f'{row["pipeline_accuracy"]:.2f}',
            _format_seconds(row["baseline_time_total"]),
            _format_seconds(row.get("baseline_time_avg", 0.0)),
            _format_seconds(row["pipeline_time_total"]),
            _format_seconds(row.get("pipeline_time_avg", 0.0)),
        ]

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in rows:
        lines.append("| " + " | ".join(row_to_cells(row)) + " |")

    lines.append(
        "| TOTAL | - | {tests} | - | - | {baseline} | {baseline_avg} | {pipeline} | {pipeline_avg} |".format(
            tests=totals["total_tests"],
            baseline=_format_seconds(totals["baseline_time_total"]),
            baseline_avg=_format_seconds(
                totals.get("baseline_time_avg_total", 0.0)),
            pipeline=_format_seconds(totals["pipeline_time_total"]),
            pipeline_avg=_format_seconds(
                totals.get("pipeline_time_avg_total", 0.0)),
        )
    )
    return "\n".join(lines)


def _escape_latex(value: Any) -> str:
    if value is None:
        return ""
    s = str(value)
    replacements = {
        "\\": "\\textbackslash{}",
        "&": "\\&",
        "%": "\\%",
        "$": "\\$",
        "#": "\\#",
        "_": "\\_",
        "{": "\\{",
        "}": "\\}",
        "~": "\\textasciitilde{}",
        "^": "\\textasciicircum{}",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    return s


def _format_latex_table(rows: list[dict[str, Any]], totals: dict[str, Any]) -> str:
    # Main table header (WITHOUT filename column)
    header = [
        "Model",
        "Tests",
        "Baseline (\\%)",
        "Pipeline (\\%)",
        "Baseline (s)",
        "Baseline avg (s)",
        "Pipeline (s)",
        "Pipeline avg (s)",
    ]

    def fmt_row(r: dict[str, Any]) -> list[str]:
        return [
            _escape_latex(r.get("model", "")),
            str(r.get("total_tests", "")),
            f'{r.get("baseline_accuracy", 0):.2f}',
            f'{r.get("pipeline_accuracy", 0):.2f}',
            _format_seconds(r.get("baseline_time_total", 0.0)),
            _format_seconds(r.get("baseline_time_avg", 0.0)),
            _format_seconds(r.get("pipeline_time_total", 0.0)),
            _format_seconds(r.get("pipeline_time_avg", 0.0)),
        ]

    # Begin LaTeX document with packages suitable to fit tables inside margins
    lines: list[str] = []
    lines.append(r"\documentclass{article}")
    lines.append(r"\usepackage[margin=1in]{geometry}")
    lines.append(r"\usepackage{booktabs}")
    lines.append(r"\usepackage[T1]{fontenc}")
    lines.append(r"\usepackage{tabularx}")
    lines.append(r"\usepackage{array}")
    lines.append(r"\usepackage{caption}")
    lines.append(r"\begin{document}")

    # First table: main results — use tabularx to ensure width fits margins
    lines.append(r"\begin{table}[ht]")
    lines.append(r"\centering")
    lines.append(r"\small")
    # model column flexible (X), others right-aligned
    lines.append(r"\begin{tabularx}{\textwidth}{X r r r r r r r}")
    lines.append(r"\toprule")
    lines.append(" & ".join(header) + r" \\")
    lines.append(r"\midrule")

    for r in rows:
        cells = fmt_row(r)
        lines.append(" & ".join(cells) + r" \\")

    # totals row omitted (total_time removed)
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabularx}")
    lines.append(r"\caption{Benchmark summary}")
    lines.append(r"\end{table}")

    lines.append(r"\end{document}")

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
        "baseline_time_avg",
        "pipeline_time_total",
        "pipeline_time_avg",
        # total_time_total removed
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


def _generate_plots(results_dir: Path, out_dir: Path) -> None:
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except Exception:
        raise SystemExit(
            "Per generare i grafici installa matplotlib: pip install matplotlib numpy")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Aggregate raw per-item data grouped by model and query_complexity
    models: dict[str, dict[str, list[dict[str, Any]]]
                 ] = defaultdict(lambda: defaultdict(list))
    for file_path in _iter_result_files(results_dir):
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        model = str(data.get("model") or file_path.stem)
        items = data.get("results") or []
        for item in items:
            complexity = str(item.get("query_complexity") or "UNKNOWN")
            models[model][complexity].append(item)

    for model_name, complexities in sorted(models.items()):
        # Sort complexities by total count descending for stable layout
        complexity_keys = sorted(
            complexities.keys(), key=lambda k: -len(complexities[k]))

        # Prepare data arrays
        labels = complexity_keys
        baseline_acc = []
        pipeline_acc = []
        match_counts = []
        mismatch_counts = []
        baseline_time_avg = []
        pipeline_time_avg = []

        for k in labels:
            items = complexities[k]
            total = len(items) or 1
            b_correct = sum(1 for it in items if bool(
                it.get("baseline_correct") or False))
            p_correct = sum(1 for it in items if bool(
                it.get("pipeline_correct") or False))
            both = sum(1 for it in items if bool(it.get("baseline_correct")
                       or False) and bool(it.get("pipeline_correct") or False))
            match = both
            mismatch = total - both

            baseline_acc.append(100.0 * b_correct / total)
            pipeline_acc.append(100.0 * p_correct / total)
            match_counts.append(match)
            mismatch_counts.append(mismatch)

            b_time_sum = sum(_as_float(it.get("baseline_time_seconds") or it.get(
                "baseline_time") or 0.0) for it in items)
            p_time_sum = sum(_as_float(it.get("pipeline_time_seconds") or it.get(
                "pipeline_time") or 0.0) for it in items)
            baseline_time_avg.append(b_time_sum / total if total else 0.0)
            pipeline_time_avg.append(p_time_sum / total if total else 0.0)

        # Plot: accuracy (top), match/mismatch (bottom-left), avg time (bottom-right)
        fig = plt.figure(constrained_layout=True, figsize=(14, 8))
        gs = fig.add_gridspec(2, 2)

        ax_acc = fig.add_subplot(gs[0, :])
        x = np.arange(len(labels))
        width = 0.35
        ax_acc.bar(x - width/2, baseline_acc, width,
                   label='Baseline', color='#2ecc71')
        ax_acc.bar(x + width/2, pipeline_acc, width,
                   label='Pipeline', color='#ff7f50')
        ax_acc.set_xticks(x)
        ax_acc.set_xticklabels(labels, rotation=45, ha='right')
        ax_acc.set_ylabel('Accuracy (%)')
        ax_acc.set_title(f'Accuracy per Query Complexity — {model_name}')
        ax_acc.legend()
        for i, v in enumerate(baseline_acc):
            ax_acc.text(i - width/2, v + 1,
                        f"{v:.1f}%", ha='center', va='bottom', fontsize=8)
        for i, v in enumerate(pipeline_acc):
            ax_acc.text(i + width/2, v + 1,
                        f"{v:.1f}%", ha='center', va='bottom', fontsize=8)

        ax_mm = fig.add_subplot(gs[1, 0])
        ax_mm.bar(x, match_counts, label='Match', color='#2ecc71')
        ax_mm.bar(x, mismatch_counts, bottom=match_counts,
                  label='Mismatch', color='#e74c3c')
        ax_mm.set_xticks(x)
        ax_mm.set_xticklabels(labels, rotation=45, ha='right')
        ax_mm.set_ylabel('Count')
        ax_mm.set_title('Match / Mismatch per Query Complexity')
        ax_mm.legend()

        ax_time = fig.add_subplot(gs[1, 1])
        ax_time.bar(x - width/2, baseline_time_avg, width,
                    label='Baseline', color='#2ecc71')
        ax_time.bar(x + width/2, pipeline_time_avg, width,
                    label='Pipeline', color='#ff7f50')
        ax_time.set_xticks(x)
        ax_time.set_xticklabels(labels, rotation=45, ha='right')
        ax_time.set_ylabel('Avg time (s)')
        ax_time.set_title('Tempo Medio di Risposta per Query Complexity')
        ax_time.legend()
        for i, v in enumerate(baseline_time_avg):
            ax_time.text(i - width/2, v + max(0.5, v*0.02),
                         f"{v:.1f}s", ha='center', va='bottom', fontsize=8)
        for i, v in enumerate(pipeline_time_avg):
            ax_time.text(i + width/2, v + max(0.5, v*0.02),
                         f"{v:.1f}s", ha='center', va='bottom', fontsize=8)

        plt.suptitle(
            f'Benchmark breakdown per Query Complexity — {model_name}', fontsize=12)

        # Save
        safe_name = model_name.replace(
            ' ', '_').replace('/', '_').replace(':', '_')
        out_path = out_dir / f"{safe_name}.png"
        fig.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)


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
        choices=("md", "csv", "json", "pdf", "tex"),
        default="md",
        help="Output format",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate per-LLM plots grouped by query_complexity (PNG files)",
    )
    parser.add_argument(
        "--plots-dir",
        type=Path,
        help="Output directory for generated plots (default: <results-dir>/plots)",
    )
    args = parser.parse_args()

    if not args.results_dir.exists():
        raise SystemExit(f"Cartella risultati non trovata: {args.results_dir}")

    rows = []
    for file_path in _iter_result_files(args.results_dir):
        summary = _load_summary(file_path, args.results_dir)
        if summary is not None:
            rows.append(summary)

    # If plotting requested, generate per-LLM plots and exit
    if args.plot:
        plots_dir = args.plots_dir or (args.results_dir / "plots")
        _generate_plots(args.results_dir, plots_dir)
        print(f"Plots generate in: {plots_dir}")
        return

    rows.sort(key=lambda row: (str(row["model"]).lower(), str(
        row["timestamp"]).lower(), str(row["file"]).lower()))

    totals = {
        "total_tests": sum(int(row["total_tests"]) for row in rows),
        "baseline_time_total": round(sum(float(row["baseline_time_total"]) for row in rows), 2),
        "pipeline_time_total": round(sum(float(row["pipeline_time_total"]) for row in rows), 2),
    }
    # total_time_total removed (users requested baseline+pipeline column removed)
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

    # Average times across all tests (weighted by total_tests)
    if totals["total_tests"]:
        totals["baseline_time_avg_total"] = round(
            totals["baseline_time_total"] / totals["total_tests"], 2)
        totals["pipeline_time_avg_total"] = round(
            totals["pipeline_time_total"] / totals["total_tests"], 2)
    else:
        totals["baseline_time_avg_total"] = 0.0
        totals["pipeline_time_avg_total"] = 0.0

    if args.format == "md":
        content = _format_markdown_table(rows, totals)
        if args.output:
            args.output.write_text(content + "\n", encoding="utf-8")
        else:
            print(content)
        return

    if args.format == "tex":
        content = _format_latex_table(rows, totals)
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
        doc.title = "Benchmark summary"

        def _set_pdf_metadata(canvas, document):
            canvas.setTitle("Benchmark summary")

        # Build table data (omit file name column for PDF)
        page_width, page_height = A4
        left_margin = doc.leftMargin
        right_margin = doc.rightMargin
        usable_width = page_width - left_margin - right_margin

        styles = getSampleStyleSheet()
        elems = []
        elems.append(Paragraph("Benchmark summary", styles["Title"]))
        elems.append(Spacer(1, 12))

        header = [
            "model",
            "tests",
            "baseline accuracy (%)",
            "pipeline accuracy (%)",
            "baseline time (s)",
            "baseline avg (s)",
            "pipeline time (s)",
            "pipeline avg (s)",
        ]
        data_table = [header]
        for r in rows:
            data_table.append([
                r.get("model", ""),
                str(r.get("total_tests", "")),
                f'{r.get("baseline_accuracy", 0):.2f}',
                f'{r.get("pipeline_accuracy", 0):.2f}',
                _format_seconds(r.get("baseline_time_total", 0.0)),
                _format_seconds(r.get("baseline_time_avg", 0.0)),
                _format_seconds(r.get("pipeline_time_total", 0.0)),
                _format_seconds(r.get("pipeline_time_avg", 0.0)),
            ])

        rel = [0.26, 0.08, 0.12, 0.12, 0.12, 0.08, 0.12, 0.10]
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

        doc.build(elems, onFirstPage=_set_pdf_metadata,
                  onLaterPages=_set_pdf_metadata)

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
