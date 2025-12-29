import xml.etree.ElementTree as ET
import json
import os
import sys
import shutil
import re
import fnmatch
from pathlib import Path
from datetime import datetime

CONFIG_PATH = "grok_config.json"
REPORT_DIR = "migration_reports"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: No se encontró {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def should_exclude_path(path: Path, excluded_dirs, excluded_files):
    for excl_dir in excluded_dirs:
        if excl_dir in path.parts:
            return True
    for pattern in excluded_files:
        if fnmatch.fnmatch(path.name, pattern):
            return True
    return False

def backup_file(file_path):
    backup_path = f"{file_path}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(file_path, backup_path)
    return backup_path

# === Migración XML general (pom.xml, persistence.xml, etc.) ===
def migrate_xml(file_path, file_type, rules, changes_log, dry_run):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        return False

    ns = {}  # Para persistence.xml, no siempre Maven ns
    if file_type == "pom.xml":
        ns = {"mvn": "http://maven.apache.org/POM/4.0.0"}
        ET.register_namespace("", ns["mvn"])

    changed = False
    for rule in rules.get(file_type, []):
        pattern = rule["Target_Pattern"]
        new_block = rule["New_Block"]

        # Para persistence.xml: buscar <property name="hibernate.dialect" value="..."/>
        if "property_name" in new_block:
            properties = root.findall(".//property")  # Asumiendo JPA persistence.xml
            for prop in properties:
                if prop.get("name") == new_block["property_name"] and re.match(pattern, prop.get("value", "")):
                    old_value = prop.get("value")
                    new_value = new_block["value"]
                    changes_log.append({
                        "file": str(file_path),
                        "type": "xml_property",
                        "old": old_value,
                        "new": new_value,
                        "description": rule.get("Description", "")
                    })
                    if not dry_run:
                        prop.set("value", new_value)
                    changed = True
        # Otros bloques XML similares (e.g., <provider>)
        elif "provider" in new_block:
            provider_elem = root.find(".//provider")
            if provider_elem is not None and re.match(pattern, provider_elem.text):
                old = provider_elem.text
                new = new_block["provider"]
                changes_log.append({
                    "file": str(file_path),
                    "type": "xml_provider",
                    "old": old,
                    "new": new,
                    "description": rule.get("Description", "")
                })
                if not dry_run:
                    provider_elem.text = new
                changed = True
        # Para pom.xml (dependencias, como antes)
        else:
            for dependency in root.findall(".//mvn:dependency", ns):
                artifact_id_elem = dependency.find("mvn:artifactId", ns)
                if not artifact_id_elem or not artifact_id_elem.text:
                    continue
                artifact_id = artifact_id_elem.text
                if (pattern.endswith("*") and artifact_id.startswith(pattern[:-1])) or artifact_id == pattern:
                    old_group = dependency.find("mvn:groupId", ns).text if dependency.find("mvn:groupId", ns) is not None else "??"
                    old = f"{old_group}:{artifact_id}"
                    new = f"{new_block['groupId']}:{new_block['artifactId']}"
                    changes_log.append({
                        "file": str(file_path),
                        "type": "dependency",
                        "old": old,
                        "new": new,
                        "description": rule.get("Description", "")
                    })
                    if not dry_run:
                        group = dependency.find("mvn:groupId", ns) or ET.SubElement(dependency, "groupId")
                        group.text = new_block["groupId"]
                        artifact_id_elem.text = new_block["artifactId"]
                        version_elem = dependency.find("mvn:version", ns)
                        if "version" in new_block:
                            if version_elem is None:
                                version_elem = ET.SubElement(dependency, "version")
                            version_elem.text = new_block["version"]
                        elif version_elem is not None:
                            dependency.remove(version_elem)
                    changed = True

    if changed and not dry_run:
        backup_file(file_path)
        tree.write(file_path, encoding="utf-8", xml_declaration=True)
    return changed

# === Migración archivos de texto (properties, yml, java, sql) ===
def migrate_text_file(file_path, rules, file_type, changes_log, dry_run):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except:
        return False

    original = content
    for rule in rules.get(file_type, []):
        search = rule["Search"]
        replace = rule["Replace"]
        new_content, count = re.subn(search, replace, content, flags=re.MULTILINE | re.DOTALL)
        if count > 0:
            changes_log.append({
                "file": str(file_path),
                "type": "text_replace",
                "old": search,
                "new": replace,
                "description": rule.get("Description", ""),
                "matches": count
            })
            content = new_content

    if content != original:
        if not dry_run:
            backup_path = backup_file(file_path)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
        return True
    return False

# === Generar reporte HTML (actualizado para nuevos tipos) ===
def generate_html_report(changes_log, dry_run, config):
    os.makedirs(REPORT_DIR, exist_ok=True)
    report_path = Path(REPORT_DIR) / config["Report Options"]["Report Filename"]

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mode = "DRY-RUN (simulación)" if dry_run else "APLICADO"

    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>Reporte Migración Oracle → PostgreSQL</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background: #f9f9f9; }}
            h1 {{ color: #2c3e50; }}
            .summary {{ background: #ecf0f1; padding: 20px; border-radius: 8px; margin-bottom: 30px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background: #3498db; color: white; }}
            tr:nth-child(even) {{ background: #f2f2f2; }}
            .badge {{ padding: 5px 10px; border-radius: 12px; color: white; }}
            .success {{ background: #27ae60; }}
            .info {{ background: #2980b9; }}
        </style>
    </head>
    <body>
        <h1>Reporte de Migración: Oracle → PostgreSQL</h1>
        <div class="summary">
            <p><strong>Fecha:</strong> {timestamp}</p>
            <p><strong>Modo:</strong> <span class="badge {'success' if not dry_run else 'info'}">{mode}</span></p>
            <p><strong>Total cambios detectados:</strong> {len(changes_log)}</p>
        </div>

        <h2>Detalles de Cambios</h2>
        <table>
            <tr>
                <th>Archivo</th>
                <th>Tipo</th>
                <th>Valor Anterior</th>
                <th>Valor Nuevo</th>
                <th>Descripción</th>
                <th>Coincidencias</th>
            </tr>
    """

    for change in changes_log:
        matches = change.get("matches", "-")
        html += f"""
            <tr>
                <td><strong>{change['file']}</strong></td>
                <td>{change['type']}</td>
                <td><code>{change['old']}</code></td>
                <td><code>{change['new']}</code></td>
                <td>{change['description']}</td>
                <td>{matches}</td>
            </tr>
        """

    html += """
        </table>
        <p style="margin-top: 50px; color: #7f8c8d;">
            Generado automáticamente por migrate_oracle_to_postgres.py
        </p>
    </body>
    </html>
    """

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nReporte HTML generado: {report_path}")
    return report_path

# === Main ===
def main():
    config = load_config()
    dry_run = "--dry-run" in sys.argv
    if "--apply" in sys.argv:
        dry_run = False
        sys.argv.remove("--apply")
    if dry_run:
        sys.argv.remove("--dry-run")

    mode_text = "DRY-RUN" if dry_run else "APPLY"
    print(f"\n=== Migración Oracle → PostgreSQL - Modo: {mode_text} ===\n")

    excluded_dirs = config["Scan Options"].get("Excluded Directories", [])
    excluded_files = config["Scan Options"].get("Excluded Files", [])

    base_paths = [Path(p).resolve() for p in sys.argv[1:]] if len(sys.argv) > 1 else [Path(".")]

    changes_log = []

    # Procesar archivos XML (pom.xml, persistence.xml)
    xml_rules = config.get("XML Migration Rules", {})
    for file_type in xml_rules.keys():
        for base in base_paths:
            for file_path in base.rglob(file_type):
                if should_exclude_path(file_path, excluded_dirs, excluded_files):
                    continue
                print(f"Procesando XML ({file_type}): {file_path}")
                migrate_xml(file_path, file_type, xml_rules, changes_log, dry_run)

    # Procesar archivos de texto (properties, yml, java, sql)
    text_rules = config.get("Text Migration Rules", {})
    for pattern in text_rules.keys():
        for base in base_paths:
            for file_path in base.rglob(pattern if not pattern.startswith("*") else pattern[2:]):  # Maneja *.java como .java
                if should_exclude_path(file_path, excluded_dirs, excluded_files):
                    continue
                print(f"Procesando texto ({pattern}): {file_path}")
                migrate_text_file(file_path, text_rules, pattern, changes_log, dry_run)

    # Generar reporte
    generate_html_report(changes_log, dry_run, config)

    print(f"\n¡Proceso completado! Total de cambios: {len(changes_log)}")

if __name__ == "__main__":
    main()