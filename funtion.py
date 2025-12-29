#!/usr/bin/env python3
"""
funtion.py
Genera reportes CSV y HTML incluso en dry-run; no escribe cambios en dry-run.
"""

import json
import re
import fnmatch
import shutil
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional


def _collect_applicable_rules(file_rules: Dict[str, List[Dict[str, Any]]], filename: str) -> List[Dict[str, Any]]:
    applicable: List[Dict[str, Any]] = []
    if filename in file_rules:
        applicable.extend(file_rules[filename])
    for key, rules in file_rules.items():
        if any(ch in key for ch in ["*", "?", "[", "]"]):
            if fnmatch.fnmatch(filename, key):
                applicable.extend(rules)
    return applicable


def _ensure_backup(base_dir: Path, backup_dir: Optional[Path]) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    if backup_dir:
        dest = backup_dir / f"backup_{ts}"
    else:
        dest = base_dir.parent / f"{base_dir.name}_backup_{ts}"
    shutil.copytree(base_dir, dest)
    return dest


def _append_csv(report_path: Path, row: Dict[str, str]) -> None:
    header = ["timestamp", "file", "rule_id", "description", "line_context", "before", "after", "dry_run"]
    write_header = not report_path.exists()
    with report_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _init_html(report_html_path: Path, dry_run: bool) -> None:
    ts = datetime.utcnow().isoformat()
    dry_note = "<div style='color:#b45f06'><strong>Nota:</strong> Dry-run activo — no se aplicaron cambios.</div>" if dry_run else ""
    html_header = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<title>Migration Report</title>
<style>
body{font-family:Arial,Helvetica,sans-serif;margin:20px}
h1{color:#2b6cb0}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #ddd;padding:8px;text-align:left}
th{background:#f4f6f8}
tr:nth-child(even){background:#fbfbfb}
.code{font-family:monospace;background:#f7f7f7;padding:4px;border-radius:3px;white-space:pre-wrap}
.summary{margin-bottom:16px}
</style>
</head>
<body>
<h1>Informe de migración</h1>
<div class="summary"><strong>Generado:</strong> """ + ts + """</div>
""" + dry_note + """
<table>
<thead><tr><th>Fichero</th><th>Regla</th><th>Descripción</th><th>Contexto</th><th>Antes</th><th>Después</th><th>Timestamp</th></tr></thead>
<tbody>
"""
    report_html_path.parent.mkdir(parents=True, exist_ok=True)
    with report_html_path.open("w", encoding="utf-8") as f:
        f.write(html_header)


def _append_html(report_html_path: Path, row: Dict[str, str]) -> None:
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    tr = "<tr><td>{file}</td><td>{rule}</td><td>{desc}</td><td class='code'>{ctx}</td><td class='code'>{before}</td><td class='code'>{after}</td><td>{ts}</td></tr>\n".format(
        file=esc(row.get("file","")),
        rule=esc(row.get("rule_id","")),
        desc=esc(row.get("description","")),
        ctx=esc(row.get("line_context","")),
        before=esc(row.get("before","")),
        after=esc(row.get("after","")),
        ts=esc(row.get("timestamp",""))
    )
    with report_html_path.open("a", encoding="utf-8") as f:
        f.write(tr)


def _finalize_html(report_html_path: Path) -> None:
    with report_html_path.open("a", encoding="utf-8") as f:
        f.write("</tbody></table>\n</body>\n</html>")


def apply_replacements_in_directory(
    config_file: str,
    base_dir: str,
    dry_run: bool = False,
    make_backup: bool = True,
    backup_dir: Optional[str] = None,
    report_file: Optional[str] = None,
    report_html: Optional[str] = None
) -> None:
    cfg_path = Path(config_file)
    base_path = Path(base_dir)

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    if not base_path.exists() or not base_path.is_dir():
        raise FileNotFoundError(f"Base directory not found or not a directory: {base_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    scan_opts = config.get("Scan Options", {})
    excluded_dirs = set(scan_opts.get("Excluded Directories", []))
    excluded_files = set(scan_opts.get("Excluded Files", []))
    search_patterns = scan_opts.get("Search_files", [])
    file_rules = config.get("File Specific Rules", {})

    # Backup: solo si no es dry-run y make_backup True
    backup_path = None
    if make_backup and not dry_run:
        backup_base = Path(backup_dir) if backup_dir else None
        backup_path = _ensure_backup(base_path, backup_base)
        print(f"🔐 Backup creado en: {backup_path}")
    else:
        if dry_run and make_backup:
            print("🔐 Dry-run: no se crea backup (backup omitido en dry-run)")

    report_path = Path(report_file) if report_file else base_path / "migration_report.txt"
    report_html_path = Path(report_html) if report_html else base_path / "migration_report.html"

    # Inicializar reportes: en dry-run queremos generar reportes, así que inicializamos ambos aunque no escribamos cambios
    if not dry_run:
        # Si no es dry-run, inicializamos CSV/HTML normalmente
        _init_html(report_html_path, dry_run=False)
        print(f"📝 Reporte HTML inicializado en: {report_html_path}")
    else:
        # Dry-run: inicializamos HTML con nota de dry-run, y creamos CSV header si no existe
        _init_html(report_html_path, dry_run=True)
        print(f"📝 Dry-run: Reporte HTML inicializado en: {report_html_path} (no se aplicarán cambios)")

    # Asegurar CSV header si no existe
    if not report_path.exists():
        header = ["timestamp", "file", "rule_id", "description", "line_context", "before", "after", "dry_run"]
        with report_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()

    for path in base_path.rglob("*"):
        if path.is_dir():
            if path.name in excluded_dirs:
                print(f"   ⛔ Saltando directorio excluido: {path}")
                continue
            else:
                continue

        if not path.is_file():
            continue
        if path.name in excluded_files:
            continue
        if search_patterns and not any(fnmatch.fnmatch(path.name, p) for p in search_patterns):
            continue

        rules = _collect_applicable_rules(file_rules, path.name)
        if not rules:
            continue

        print(f"\n📄 Procesando {path}")
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            print(f"   ⚠️ Archivo no texto o codificación no UTF-8: se omite {path.name}")
            continue

        original_content = content
        changes_made = False

        for rule in rules:
            oldval = rule.get("Oldval", "")
            newval = rule.get("Newval", "")
            rule_id = rule.get("ID", "(sin ID)")
            description = rule.get("Description", "")

            if not oldval:
                print(f"   ⚠️ Regla {rule_id} sin 'Oldval': se omite")
                continue

            try:
                pattern = re.compile(oldval, flags=re.MULTILINE | re.DOTALL)
            except re.error as e:
                print(f"   ❌ Regex inválida en regla {rule_id}: {e}")
                continue

            matches = list(pattern.finditer(content))
            if matches:
                changes_made = True
                print(f"   🔎 Regla: {rule_id} - {description}")
                for m in matches:
                    before = m.group(0)
                    try:
                        after = pattern.sub(newval, before)
                    except Exception:
                        after = newval
                    start_line = content.rfind("\n", 0, m.start()) + 1
                    end_line = content.find("\n", m.end())
                    if end_line == -1:
                        end_line = len(content)
                    line_text = content[start_line:end_line].strip()
                    print(f"      Línea: {line_text}")
                    print(f"      Antes: {before}")
                    print(f"      Después: {after}")

                    row = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "file": str(path.relative_to(base_path)),
                        "rule_id": rule_id,
                        "description": description,
                        "line_context": line_text,
                        "before": before,
                        "after": after,
                        "dry_run": "true" if dry_run else "false"
                    }

                    # Siempre registramos en reportes, incluso en dry-run
                    _append_csv(report_path, row)
                    _append_html(report_html_path, row)

                # Aplicar reemplazo en contenido solo si no es dry-run
                if not dry_run:
                    try:
                        content = pattern.sub(newval, content)
                    except Exception as e:
                        print(f"   ❌ Error aplicando reemplazo global para regla {rule_id}: {e}")
                else:
                    print("   ℹ️ Dry-run: cambio propuesto no aplicado al fichero")
            else:
                print(f"   ➖ Sin coincidencias para regla {rule_id} en {path.name}")

        if changes_made:
            if dry_run:
                print(f"   ℹ️ Dry-run: no se guardan cambios en {path.name}")
            else:
                try:
                    path.write_text(content, encoding="utf-8")
                    print(f"   ✅ Cambios guardados en {path.name}")
                except Exception as e:
                    print(f"   ❌ Error guardando {path.name}: {e}")
        else:
            print(f"   ℹ️ No se realizaron cambios en {path.name}")

    # Finalizar HTML
    _finalize_html(report_html_path)
    print(f"🟢 Reporte HTML finalizado en: {report_html_path}")
    print(f"📄 Informe CSV: {report_path}")
    print("\n🎯 Proceso completado.")
