#!/usr/bin/env python3

import json
import re
import fnmatch
import shutil
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from lxml import etree


# Configuración del logger para este módulo
logger = logging.getLogger(__name__)

class ReportGenerator:
    """Clase encargada exclusivamente de generar reportes CSV y HTML."""

    def __init__(self, base_path: Path, csv_path: Optional[str] = None, html_path: Optional[str] = None):
        self.base_path = base_path
        self.csv_path = Path(csv_path) if csv_path else base_path / "migration_report.csv"
        self.html_path = Path(html_path) if html_path else base_path / "migration_report.html"
        self.csv_header = ["timestamp", "file", "rule_id", "description", "line_context", "before", "after", "dry_run"]

    def init_reports(self, dry_run: bool) -> None:
        """Inicializa los archivos de reporte."""
        # CSV Init
        if not self.csv_path.exists():
            try:
                with self.csv_path.open("w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.csv_header)
                    writer.writeheader()
            except IOError as e:
                logger.error(f"No se pudo crear CSV {self.csv_path}: {e}")

        # HTML Init
        ts = datetime.utcnow().isoformat()
        dry_note = "<div style='background:#fff3cd; color:#856404; padding:10px; margin-bottom:10px; border:1px solid #ffeeba;'><strong>⚠️ MODO DRY-RUN:</strong> No se han aplicado cambios reales.</div>" if dry_run else ""
        
        html_content = f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<title>Informe de Migración Oracle a PostgreSQL</title>
<style>
    body{{font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin:20px; background-color:#f8f9fa; color:#333;}}
    h1{{color:#0056b3; border-bottom: 2px solid #dee2e6; padding-bottom: 10px;}}
    .summary{{background:#fff; padding:15px; border-radius:5px; box-shadow:0 2px 4px rgba(0,0,0,0.1); margin-bottom:20px;}}
    table{{border-collapse:collapse; width:100%; background:#fff; box-shadow:0 2px 4px rgba(0,0,0,0.05);}}
    th,td{{border:1px solid #dee2e6; padding:12px; text-align:left; font-size:0.9rem;}}
    th{{background-color:#e9ecef; color:#495057; font-weight:600;}}
    tr:nth-child(even){{background-color:#f8f9fa;}}
    tr:hover{{background-color:#e2e6ea;}}
    .code{{font-family: Consolas, Monaco, 'Andale Mono', monospace; background:#f1f3f5; padding:2px 4px; border-radius:3px; color:#d63384; font-size:0.85rem;}}
    .diff-del{{background-color:#ffeef0; text-decoration: line-through; color: #a61b1b;}}
    .diff-add{{background-color:#e6fffa; color: #047481;}}
</style>
</head>
<body>
<h1>Informe de Migración</h1>
<div class="summary">
    <strong>Fecha:</strong> {ts}<br>
    <strong>Directorio Base:</strong> {self.base_path}
</div>
{dry_note}
<table>
<thead><tr><th>Fichero</th><th>Regla</th><th>Contexto</th><th>Antes</th><th>Después</th></tr></thead>
<tbody>
"""
        try:
            self.html_path.parent.mkdir(parents=True, exist_ok=True)
            with self.html_path.open("w", encoding="utf-8") as f:
                f.write(html_content)
        except IOError as e:
            logger.error(f"No se pudo crear HTML {self.html_path}: {e}")

    def append_entry(self, row: Dict[str, str]) -> None:
        """Agrega una entrada a ambos reportes."""
        # CSV
        try:
            with self.csv_path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_header)
                writer.writerow(row)
        except IOError:
            pass

        # HTML
        def esc(s: str) -> str:
            return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        
        tr = f"""<tr>
            <td>{esc(row['file'])}</td>
            <td><strong>{esc(row['rule_id'])}</strong><br><small>{esc(row['description'])}</small></td>
            <td class='code'>{esc(row['line_context'])}</td>
            <td class='code diff-del'>{esc(row['before'])}</td>
            <td class='code diff-add'>{esc(row['after'])}</td>
        </tr>"""
        
        try:
            with self.html_path.open("a", encoding="utf-8") as f:
                f.write(tr)
        except IOError:
            pass

    def finalize(self) -> None:
        """Cierra los tags del HTML."""
        try:
            with self.html_path.open("a", encoding="utf-8") as f:
                f.write("</tbody></table><div style='margin-top:20px; text-align:center; color:#777;'>Generado por herramienta de migración</div></body></html>")
            logger.info(f"📄 Reporte HTML generado: {self.html_path}")
            logger.info(f"📊 Reporte CSV generado: {self.csv_path}")
        except IOError:
            pass




class MigrationEngine:
    def __init__(self, config_file: str, base_dir: str, dry_run: bool = False):
        self.config_file = Path(config_file)
        self.base_dir = Path(base_dir)
        self.dry_run = dry_run
        self.config = self._load_config()
        # Inicializamos el generador de reportes
        self.report = ReportGenerator(self.base_dir)
        
    def _load_config(self) -> Dict:
        with open(self.config_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _process_xml_file(self, path: Path, rules: List[Dict]) -> bool:
        try:
            parser = etree.XMLParser(remove_blank_text=False, recover=True)
            tree = etree.parse(str(path), parser)
            root = tree.getroot()
            nsmap = root.nsmap.copy()
            ns = {"mvn": nsmap.get(None)} if nsmap.get(None) else {}
            changed = False

            for rule in rules:
                pattern = rule["Target_Pattern"]
                new_data = rule["New_Block"]
                xpath_query = "//mvn:dependency" if ns else "//dependency"
                
                for dep in root.xpath(xpath_query, namespaces=ns):
                    art_node = dep.find("mvn:artifactId", ns) if ns else dep.find("artifactId")
                    
                    if art_node is not None and fnmatch.fnmatch(art_node.text.strip(), pattern):
                        print(f"   🎯 MATCH XML: Encontrado {art_node.text.strip()} en {path.name}")
                        for tag, value in new_data.items():
                            node = dep.find(f"mvn:{tag}", ns) if ns else dep.find(tag)
                            if node is not None:
                                before_val = node.text
                                node.text = value
                                # Registrar en reporte
                                self.report.append_entry({
                                    "timestamp": datetime.now().isoformat(),
                                    "file": str(path.name),
                                    "rule_id": rule["ID"],
                                    "description": f"Cambio de {tag}",
                                    "line_context": f"Dependency {art_node.text}",
                                    "before": before_val,
                                    "after": value,
                                    "dry_run": str(self.dry_run)
                                })
                                changed = True
            
            if changed and not self.dry_run:
                tree.write(str(path), encoding="utf-8", xml_declaration=True, pretty_print=False)
            return changed
        except Exception as e:
            print(f"   ❌ Error XML: {e}")
            return False

    def _process_regex_file(self, path: Path, rules: List[Dict]) -> bool:
        try:
            content = path.read_text(encoding="utf-8")
            original_content = content
            
            for rule in rules:
                clean_new_val = re.sub(r'\$(\d+)', r'\\g<\1>', rule["Newval"])
                pattern = re.compile(rule["Oldval"], flags=re.MULTILINE | re.DOTALL | re.IGNORECASE)
                
                # Buscamos coincidencias para el reporte
                for match in pattern.finditer(content):
                    print(f"   [Regex] Match en {path.name}: {rule['ID']}")
                    before_text = match.group(0)
                    # Simulamos el reemplazo para el reporte
                    after_text = pattern.sub(clean_new_val, before_text)
                    
                    self.report.append_entry({
                        "timestamp": datetime.now().isoformat(),
                        "file": str(path.name),
                        "rule_id": rule["ID"],
                        "description": rule["Description"],
                        "line_context": before_text[:50].replace('\n', ' '),
                        "before": before_text,
                        "after": after_text,
                        "dry_run": str(self.dry_run)
                    })

                content = pattern.sub(clean_new_val, content)
            
            changed = content != original_content
            if changed and not self.dry_run:
                path.write_text(content, encoding="utf-8")
            return changed
        except Exception as e:
            print(f"   ❌ Error Regex: {e}")
            return False

    def run_migration(self):
        """Recorre archivos y decide qué motor usar."""
        self.report.init_reports(self.dry_run)
        # Forzamos los nombres exactos que tienes en tu JSON
        search_patterns = self.config.get("Scan Options", {}).get("Search_files", [])
        
        # AJUSTE: Mapeamos los nombres de tu JSON a lo que el código espera
        xml_rules_map = self.config.get("POM Migration Rules", {}) 
        regex_rules_map = self.config.get("File Specific Rules", {})

        print(f"🚀 Iniciando escaneo en: {self.base_dir}")
        print(f"🔍 Buscando archivos que coincidan con: {search_patterns}")

        archivos_encontrados = 0
        
        # Usamos resolve() para limpiar las rutas de Windows/Linux
        base_path = self.base_dir.resolve()

        for path in base_path.rglob("*"):
            # 1. Ignorar si es carpeta
            if not path.is_file():
                continue

            # 2. Ignorar si está en carpetas excluidas
            excluded_dirs = self.config.get("Scan Options", {}).get("Excluded Directories", [])
            if any(exc in path.parts for exc in excluded_dirs):
                continue

            # 3. Verificar si el archivo nos interesa
            if not any(fnmatch.fnmatch(path.name, p) for p in search_patterns):
                continue
            
            # SI LLEGA AQUÍ, EL ARCHIVO ES VÁLIDO
            archivos_encontrados += 1
            print(f"📂 Procesando archivo: {path.name}")
            
            changed = False
            
            # Aplicar reglas XML
            xml_rules = self._get_applicable_rules(path.name, xml_rules_map)
            if xml_rules:
                print(f"   -> Ejecutando motor XML...")
                changed = self._process_xml_file(path, xml_rules)

            # Aplicar reglas Regex
            regex_rules = self._get_applicable_rules(path.name, regex_rules_map)
            if regex_rules:
                print(f"   -> Ejecutando motor Regex...")
                changed = self._process_regex_file(path, regex_rules) or changed

            if changed:
                logger.info(f"✨ Cambios detectados en: {path.name}")

        print(f"🏁 Escaneo finalizado. Total archivos procesados: {archivos_encontrados}")
        self.report.finalize()
        
    def _get_applicable_rules(self, filename: str, rules_dict: Dict) -> List[Dict]:
        applicable = []
        for key, rules in rules_dict.items():
            if fnmatch.fnmatch(filename, key):
                applicable.extend(rules)
        return applicable

# CORRECCIÓN DEL WRAPPER FINAL:
def apply_replacements_in_directory(**kwargs):
    # 1. Crear el motor
    engine = MigrationEngine(
        config_file=kwargs.get('config_file'),
        base_dir=kwargs.get('base_dir'),
        dry_run=kwargs.get('dry_run', False)
    )
    
    # 2. Configurar rutas de reporte manualmente si vienen del main
    if kwargs.get('report_file'):
        engine.report.csv_path = Path(kwargs.get('report_file'))
    if kwargs.get('report_html'):
        engine.report.html_path = Path(kwargs.get('report_html'))

    # 3. EJECUTAR
    print(f"DEBUG: Llamando a engine.run_migration()...")
    engine.run_migration()