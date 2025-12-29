import json
import re
import os
import hashlib
from pathlib import Path
from dataclasses import dataclass, asdict, is_dataclass
from typing import Optional, List, Dict, Tuple
import xml.etree.ElementTree as ET
import difflib


# -----------------------------
# Dataclasses genéricas
# -----------------------------

@dataclass
class Rule:
    id: str
    severity: str
    description: str
    detect_regex: re.Pattern
    convert_enabled: bool
    convert_old: Optional[str]
    convert_new: Optional[str]
    category: Optional[str] = None
    domain: Optional[str] = None  # SQL, PLSQL, JAVA, FILE, etc.


@dataclass
class MatchOccurrence:
    file: str
    rule_id: str
    severity: str
    description: str
    domain: str
    category: Optional[str]
    line: int
    column: int
    snippet: str


@dataclass
class FileChange:
    file: str
    rule_id: str
    severity: str
    description: str
    domain: str
    occurrences: int
    old_value: str
    new_value: str


@dataclass
class FileAudit:
    file: str
    before_hash: Optional[str]
    after_hash: Optional[str]
    changed: bool


# -----------------------------
# Carga de configuración
# -----------------------------

def load_config(config_path: str) -> Dict:
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"No existe configuración: {config_path}")
    return json.loads(p.read_text(encoding="utf-8"))

def serialize_item(item):
    """
    Convierte dataclass -> dict; si ya es dict u otro tipo serializable, lo devuelve tal cual.
    """
    if is_dataclass(item):
        return asdict(item)
    return item


def compile_rules(config: Dict) -> Dict[str, List[Rule]]:
    """
    Compila todas las reglas en objetos Rule, agrupados por dominio:
      - file_specific
      - sql
      - plsql
      - java
    """
    compiled = {
        "file_specific": {},  # por extension/nombre: { key: [Rule, ...] }
        "sql": [],
        "plsql": [],
        "java": []
    }

    # FileSpecificRules
    fs_rules = config.get("FileSpecificRules", {})
    for key, rules in fs_rules.items():
        compiled["file_specific"].setdefault(key, [])
        for r in rules:
            detect_re = r["Detect"]["Regexp"]
            compiled["file_specific"][key].append(
                Rule(
                    id=r["ID"],
                    severity=r.get("Severity", "INFO"),
                    description=r.get("Description", ""),
                    detect_regex=re.compile(detect_re),
                    convert_enabled=r.get("Convert", {}).get("Enabled", False),
                    convert_old=r.get("Convert", {}).get("Old"),
                    convert_new=r.get("Convert", {}).get("New"),
                    category=None,
                    domain="FILE"
                )
            )

    # SQLRules
    for r in config.get("SQLRules", []):
        detect_re = r["Detect"]["Regexp"]
        compiled["sql"].append(
            Rule(
                id=r["ID"],
                severity=r.get("Severity", "INFO"),
                description=r.get("Description", ""),
                detect_regex=re.compile(detect_re, re.DOTALL),
                convert_enabled=r.get("Convert", {}).get("Enabled", False),
                convert_old=r.get("Convert", {}).get("Old"),
                convert_new=r.get("Convert", {}).get("New"),
                category=r.get("Category"),
                domain="SQL"
            )
        )

    # PLSQLRules
    for r in config.get("PLSQLRules", []):
        detect_re = r["Detect"]["Regexp"]
        compiled["plsql"].append(
            Rule(
                id=r["ID"],
                severity=r.get("Severity", "INFO"),
                description=r.get("Description", ""),
                detect_regex=re.compile(detect_re, re.DOTALL),
                convert_enabled=r.get("Convert", {}).get("Enabled", False),
                convert_old=r.get("Convert", {}).get("Old"),
                convert_new=r.get("Convert", {}).get("New"),
                category=None,
                domain="PLSQL"
            )
        )

    # JavaTypeRules
    for r in config.get("JavaTypeRules", []):
        detect_re = r["Detect"]["Regexp"]
        compiled["java"].append(
            Rule(
                id=r["ID"],
                severity=r.get("Severity", "INFO"),
                description=r.get("Description", ""),
                detect_regex=re.compile(detect_re),
                convert_enabled=r.get("Convert", {}).get("Enabled", False),
                convert_old=r.get("Convert", {}).get("Old"),
                convert_new=r.get("Convert", {}).get("New"),
                category=None,
                domain="JAVA"
            )
        )

    return compiled


# -----------------------------
# Utilidades
# -----------------------------

def sha256_of_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def find_line_col(content: str, index: int) -> Tuple[int, int, str]:
    """
    Dado un índice en el string, devuelve (linea, columna, texto_de_linea).
    """
    lines = content.splitlines(keepends=True)
    acc = 0
    for i, line in enumerate(lines):
        if acc + len(line) > index:
            col = index - acc + 1
            return i + 1, col, line.rstrip("\n\r")
        acc += len(line)
    return 1, 1, ""


def matches_search_pattern(filename: str, patterns: List[str]) -> bool:
    """
    patterns soporta glob tipo '*.java', 'pom.xml', etc.
    """
    path = Path(filename)
    for pat in patterns:
        if path.match(pat):
            return True
    return False


# -----------------------------
# Detección y conversión
# -----------------------------

def detect_in_content(content: str, rules: List[Rule], file_path: str) -> List[MatchOccurrence]:
    occurrences: List[MatchOccurrence] = []
    lines = content.splitlines()

    for rule in rules:
        for m in rule.detect_regex.finditer(content):
            line, col, snippet = find_line_col(content, m.start())

            # snippet es solo el match → lo reemplazamos por la línea completa
            full_line = lines[line - 1]

            # contexto
            before = lines[line - 2] if line - 2 >= 0 else ""
            after = lines[line] if line < len(lines) else ""

            occurrences.append({
                "file": file_path,
                "rule_id": rule.id,
                "severity": rule.severity,
                "description": rule.description,
                "domain": rule.domain,
                "category": rule.category,
                "line": line,
                "column": col,

                # 🔥 AQUÍ ESTÁ LA CLAVE
                "original_line": full_line,
                "new_line": full_line,   # se actualizará en apply_conversions

                "context_before": before,
                "context_after": after,
                "diff": None
            })

    return occurrences



def apply_pom_dependency_changes(content: str, file_path: str, occurrences: list, rules: list):
    """
    Parsea el pom.xml, busca <dependency> con artifactId matching rules,
    actualiza artifactId/version según reglas y actualiza occurrences (new_line, diff).
    Devuelve nuevo contenido y lista de cambios (summary).
    """
    changes = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        # Si no es XML válido, no tocar
        return content, changes

    ns = ""
    # detectar namespace si existe
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"
    deps = root.findall(".//{}dependency".format(ns))

    for dep in deps:
        gid = dep.find("{}groupId".format(ns))
        aid = dep.find("{}artifactId".format(ns))
        ver = dep.find("{}version".format(ns))

        gid_text = gid.text.strip() if gid is not None and gid.text else ""
        aid_text = aid.text.strip() if aid is not None and aid.text else ""
        ver_text = ver.text.strip() if ver is not None and ver.text else ""

        # buscar reglas que apliquen a este artifactId o groupId
        for rule in rules:
            # regla para artifactId
            if rule.id.lower().startswith("pom_artifactid") and rule.convert_enabled:
                old = rule.convert_old
                new = rule.convert_new
                if old and old == aid_text:
                    # actualizar artifactId
                    old_line = ET.tostring(aid, encoding="unicode").strip()
                    aid.text = new
                    new_line = ET.tostring(aid, encoding="unicode").strip()

                    # actualizar occurrences relacionadas
                    for occ in occurrences:
                        if occ.get("rule_id") == rule.id and occ.get("original_line", "").strip() == old_line:
                            occ["new_line"] = new_line
                            occ["diff"] = "\n".join(difflib.unified_diff(
                                old_line.splitlines(), new_line.splitlines(), lineterm=""
                            ))

                    changes.append({
                        "file": file_path,
                        "rule_id": rule.id,
                        "old_value": old,
                        "new_value": new
                    })

            # regla para version (aplicar solo si artifactId coincide)
            if rule.id.lower().startswith("pom_version") and rule.convert_enabled:
                old = rule.convert_old
                new = rule.convert_new
                # aplicar solo si artifactId coincide con la regla (puedes ajustar condición)
                if old and ver is not None and aid_text and any(r.id.lower().startswith("pom_artifactid") and r.convert_old == aid_text for r in rules):
                    if ver_text == old:
                        old_line = ET.tostring(ver, encoding="unicode").strip()
                        ver.text = new
                        new_line = ET.tostring(ver, encoding="unicode").strip()

                        for occ in occurrences:
                            if occ.get("rule_id") == rule.id and occ.get("original_line", "").strip() == old_line:
                                occ["new_line"] = new_line
                                occ["diff"] = "\n".join(difflib.unified_diff(
                                    old_line.splitlines(), new_line.splitlines(), lineterm=""
                                ))

                        changes.append({
                            "file": file_path,
                            "rule_id": rule.id,
                            "old_value": old,
                            "new_value": new
                        })

    # serializar de nuevo a string (preserva estructura XML, no indentación exacta)
    new_content = ET.tostring(root, encoding="unicode")
    return new_content, changes


def apply_conversions(content, rules, file_path, occurrences):
    new_content = content
    changes = []
    lines = content.splitlines()

    if file_path.endswith("pom.xml"):
        pom_new_content, pom_changes = apply_pom_dependency_changes(content, file_path, occurrences, rules)
        new_content = pom_new_content
        changes.extend(pom_changes)

    for rule in rules:
        if not rule.convert_enabled or not rule.convert_old or rule.convert_new is None:
            continue

        old = rule.convert_old
        new = rule.convert_new

        if old not in new_content:
            continue

        # aplicar reemplazo global
        new_content = new_content.replace(old, new)

        # actualizar ocurrencias
        for occ in occurrences:
            if occ["rule_id"] == rule.id:

                original = occ.get("original_line", "")

                # si la línea original contiene el patrón
                if old in original:
                    new_line = original.replace(old, new)
                else:
                    # si no coincide, la nueva línea es igual a la original
                    new_line = original

                occ["new_line"] = new_line

                # generar diff
                diff = "\n".join(
                    difflib.unified_diff(
                        original.splitlines(),
                        new_line.splitlines(),
                        lineterm=""
                    )
                )
                occ["diff"] = diff

        # registrar cambio global
        changes.append({
            "file": file_path,
            "rule_id": rule.id,
            "severity": rule.severity,
            "description": rule.description,
            "domain": rule.domain,
            "occurrences": new_content.count(new),
            "old_value": old,
            "new_value": new
        })

    return new_content, changes





# -----------------------------
# Procesado de un fichero
# -----------------------------

def process_file(
    file_path: Path,
    compiled_rules: Dict[str, List[Rule]],
    file_specific_rules: Dict[str, List[Rule]],
    search_files: List[str],
    backup_ext: str,
    dry_run: bool
) -> Tuple[List[MatchOccurrence], List[FileChange], Optional[FileAudit]]:
    rel_path = str(file_path)
    if not matches_search_pattern(rel_path, search_files):
        return [], [], None

    content = file_path.read_text(encoding="utf-8", errors="ignore")
    before_hash = sha256_of_text(content)

    # Determinar reglas por fichero
    file_name = file_path.name
    ext = file_path.suffix.lstrip(".") if file_path.suffix else ""

    rules_to_apply: List[Rule] = []

    # Reglas específicas por nombre
    if file_name in file_specific_rules:
        rules_to_apply.extend(file_specific_rules[file_name])

    # Reglas específicas por extensión
    if ext in file_specific_rules:
        rules_to_apply.extend(file_specific_rules[ext])

    # Dominio por extensión para SQL/PLSQL/Java
    domain_rules: List[Rule] = []
    if file_name.endswith(".java"):
        domain_rules.extend(compiled_rules["java"])
    elif file_name.endswith(".sql"):
        domain_rules.extend(compiled_rules["sql"])
        domain_rules.extend(compiled_rules["plsql"])
    else:
        pass

    all_rules = rules_to_apply + domain_rules

    # Detección
    occurrences = detect_in_content(content, all_rules, rel_path)

    # 🔥 Siempre simulamos las conversiones para rellenar new_line y diff en occurrences
    new_content, changes = apply_conversions(content, all_rules, rel_path, occurrences)
    changed = new_content != content
    after_hash = sha256_of_text(new_content) if changed else before_hash

    # Si es dry_run, NO escribimos archivos, pero devolvemos las ocurrencias y el audit con after_hash simulado
    if dry_run:
        return occurrences, changes, FileAudit(
            file=rel_path,
            before_hash=before_hash,
            after_hash=after_hash,
            changed=changed
        )

    # Si no es dry_run, hacemos backup y escribimos si hubo cambios
    if changed:
        backup_path = file_path.with_suffix(file_path.suffix + backup_ext)
        backup_path.write_text(content, encoding="utf-8")
        file_path.write_text(new_content, encoding="utf-8")

    audit = FileAudit(
        file=rel_path,
        before_hash=before_hash,
        after_hash=after_hash,
        changed=changed
    )

    return occurrences, changes, audit



# -----------------------------
# Escaneo del proyecto
# -----------------------------

def scan_project(config_path: str, apply_changes: bool = False) -> Dict:
    config = load_config(config_path)
    compiled = compile_rules(config)

    scan_opts = config.get("ScanOptions", {})
    global_opts = config.get("GlobalOptions", {})

    root_dir = scan_opts.get("RootDirectory", ".")
    excluded_dirs = set(scan_opts.get("ExcludedDirectories", []))
    excluded_files = set(scan_opts.get("ExcludedFiles", []))
    search_files = scan_opts.get("SearchFiles", [])

    backup_ext = global_opts.get("DefaultBackupExtension", ".bak")
    default_dry_run = global_opts.get("DefaultDryRun", True)

    dry_run = default_dry_run and not apply_changes

    base_path = Path(root_dir)

    all_occurrences: List[MatchOccurrence] = []
    all_changes: List[FileChange] = []
    all_audits: List[FileAudit] = []

    file_specific_rules: Dict[str, List[Rule]] = compiled["file_specific"]

    for root, dirs, files in os.walk(base_path):
        # filtrar directorios
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for f in files:
            if f in excluded_files:
                continue
            fp = Path(root) / f
            occ, chg, aud = process_file(
                fp,
                compiled_rules=compiled,
                file_specific_rules=file_specific_rules,
                search_files=search_files,
                backup_ext=backup_ext,
                dry_run=dry_run
            )
            all_occurrences.extend(occ)
            all_changes.extend(chg)
            if aud:
                all_audits.append(aud)

    # montar reporte estructurado
    result = {
        "dry_run": dry_run,
        "occurrences": all_occurrences,
        "changes": [serialize_item(c) for c in all_changes],
        "audit": [serialize_item(a) for a in all_audits]
    }

    # guardar JSON
    report_json = global_opts.get("ReportJson", "migration_report.json")
    Path(report_json).write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    # generar HTML
    report_html = global_opts.get("ReportHtml", "migration_report.html")
    html = generate_html_report(result)
    Path(report_html).write_text(html, encoding="utf-8")

    return result


# -----------------------------
# Informe HTML sencillo
# -----------------------------

def generate_html_report(result: Dict) -> str:
    dry_run = result.get("dry_run", True)
    occurrences = result.get("occurrences", [])
    changes = result.get("changes", [])
    audits = result.get("audit", [])

    severity_order = {"BLOCKER": 1, "MAJOR": 2, "MINOR": 3, "INFO": 4}
    occurrences_sorted = sorted(
        occurrences,
        key=lambda x: (
            severity_order.get(x.get("severity", "INFO"), 99),
            x.get("file", ""),
            x.get("line", 0)
        )
    )

    html = []
    html.append("<!DOCTYPE html><html><head><meta charset='utf-8'>")
    html.append("<title>Migration Report</title>")
    html.append("<style>")
    html.append("body { font-family: Arial, sans-serif; font-size: 14px; }")
    html.append("table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }")
    html.append("th, td { border: 1px solid #ccc; padding: 4px 8px; }")
    html.append("th { background: #f0f0f0; }")
    html.append(".BLOCKER { background-color: #ffcccc; }")
    html.append(".MAJOR { background-color: #ffe0b3; }")
    html.append(".MINOR { background-color: #ffffcc; }")
    html.append(".INFO { background-color: #e6f7ff; }")
    html.append("</style></head><body>")

    html.append("<h1>Informe de migración Oracle → PostgreSQL</h1>")
    html.append(f"<p>Modo: {'DRY-RUN (solo análisis)' if dry_run else 'APPLY (se han aplicado cambios)'}</p>")

    # Resumen
    html.append("<h2>Resumen</h2>")
    html.append("<ul>")
    html.append(f"<li>Ocurrencias detectadas: {len(occurrences)}</li>")
    html.append(f"<li>Cambios realizados: {len(changes)}</li>")
    html.append(f"<li>Ficheros auditados: {len(audits)}</li>")
    html.append("</ul>")

    # Tabla de ocurrencias
    html.append("<h2>Ocurrencias</h2>")
    html.append("<table>")
    html.append("<tr>"
                "<th>Severidad</th>"
                "<th>Regla</th>"
                "<th>Archivo</th>"
                "<th>Línea</th>"
                "<th>Original</th>"
                "<th>Nueva</th>"
                "<th>Contexto</th>"
                "<th>Diff</th>"
                "</tr>")

    for o in occurrences_sorted:
        diff_html = f"<pre style='color:#d14;'>{html_escape(o.get('diff',''))}</pre>" if o.get("diff") else ""

        context = (
            f"<pre>"
            f"{html_escape(o.get('context_before',''))}\n"
            f">>> {html_escape(o.get('original_line',''))}\n"
            f"{html_escape(o.get('context_after',''))}"
            f"</pre>"
        )

        html.append(
            f"<tr class='{o.get('severity','INFO')}'>"
            f"<td>{o.get('severity')}</td>"
            f"<td>{o.get('rule_id')}</td>"
            f"<td>{o.get('file')}</td>"
            f"<td>{o.get('line')}</td>"
            f"<td><pre>{html_escape(o.get('original_line',''))}</pre></td>"
            f"<td><pre>{html_escape(o.get('new_line',''))}</pre></td>"
            f"<td>{context}</td>"
            f"<td>{diff_html}</td>"
            "</tr>"
        )

    html.append("</table>")


    # Tabla de cambios
    html.append("<h2>Cambios realizados</h2>")
    html.append("<table>")
    html.append("<tr>"
                "<th>Severidad</th>"
                "<th>Regla</th>"
                "<th>Fichero</th>"
                "<th>Dominio</th>"
                "<th>Ocurrencias</th>"
                "<th>Old</th>"
                "<th>New</th>"
                "<th>Línea original</th>"
                "<th>Línea nueva</th>"
                "</tr>")

    for c in changes:
        sev = c.get("severity", "INFO")

        original_lines = "<br>".join(html_escape(l) for l in c.get("original_lines", []))
        new_lines = "<br>".join(html_escape(l) for l in c.get("new_lines", []))

        html.append(
            f"<tr class='{sev}'>"
            f"<td>{sev}</td>"
            f"<td>{c.get('rule_id')}</td>"
            f"<td>{c.get('file')}</td>"
            f"<td>{c.get('domain')}</td>"
            f"<td>{c.get('occurrences')}</td>"
            f"<td><pre>{html_escape(c.get('old_value',''))}</pre></td>"
            f"<td><pre>{html_escape(c.get('new_value',''))}</pre></td>"
            f"<td><pre>{original_lines}</pre></td>"
            f"<td><pre>{new_lines}</pre></td>"
            "</tr>"
        )

    html.append("</table>")

    # Tabla de auditoría
    html.append("<h2>Auditoría de ficheros</h2>")
    html.append("<table>")
    html.append("<tr><th>Fichero</th><th>Cambiado</th><th>Hash Antes</th><th>Hash Después</th></tr>")
    for a in audits:
        html.append(
            "<tr>"
            f"<td>{a.get('file')}</td>"
            f"<td>{'Sí' if a.get('changed') else 'No'}</td>"
            f"<td><code>{a.get('before_hash')}</code></td>"
            f"<td><code>{a.get('after_hash') or ''}</code></td>"
            "</tr>"
        )
    html.append("</table>")

    html.append("</body></html>")
    return "".join(html)


def html_escape(text):
    if text is None:
        return ""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )


# -----------------------------
# CLI
# -----------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Motor de migración Oracle→PostgreSQL basado en JSON de reglas."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Ruta al JSON maestro de configuración."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Aplicar cambios (por defecto solo análisis / dry-run)."
    )

    args = parser.parse_args()

    result = scan_project(args.config, apply_changes=args.apply)

    print(f"Análisis completado. Dry-run: {result['dry_run']}")
    print("Reportes generados:",
          "JSON:", load_config(args.config)["GlobalOptions"].get("ReportJson", "migration_report.json"),
          "HTML:", load_config(args.config)["GlobalOptions"].get("ReportHtml", "migration_report.html"))
