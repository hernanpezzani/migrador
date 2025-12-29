import json
import re
import fnmatch
import html
from pathlib import Path
from datetime import datetime
from lxml import etree

# --- FUNCIONES DE REPORTE ---

def iniciar_reporte_html(ruta_html, base_dir, dry_run):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    modo_alert = "⚠️ MODO DRY-RUN: No se han aplicado cambios reales." if dry_run else "✅ MODO EJECUCIÓN: Cambios aplicados."
    
    html_head = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; margin: 20px; background-color: #f4f7f9; }}
        h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .summary {{ background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
        .alert {{ background: #fff3cd; color: #856404; padding: 15px; border: 1px solid #ffeeba; border-radius: 4px; margin-bottom: 20px; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; table-layout: fixed; background: white; }}
        th, td {{ padding: 10px; border: 1px solid #e1e4e8; vertical-align: top; word-wrap: break-word; }}
        th {{ background-color: #3498db; color: white; text-align: left; }}
        .code {{ font-family: 'Consolas', monospace; font-size: 0.85em; white-space: pre-wrap; margin: 0; display: block; }}
        .diff-del {{ background-color: #ffdce0; color: #af1f24; }}
        .diff-add {{ background-color: #dcffe4; color: #1a7f37; }}
        /* Definición de anchos de columna */
        .col-file {{ width: 15%; }}
        .col-rule {{ width: 15%; }}
        .col-content {{ width: 35%; }}
    </style>
</head>
<body>
    <h1>Informe de Migración Oracle a PostgreSQL</h1>
    <div class="summary"><strong>Fecha:</strong> {ts}<br><strong>Directorio:</strong> {base_dir}</div>
    <div class="alert">{modo_alert}</div>
    <table>
        <thead>
            <tr>
                <th class="col-file">Archivo</th>
                <th class="col-rule">Regla / ID</th>
                <th class="col-content">Antes (Oracle)</th>
                <th class="col-content">Después (PostgreSQL)</th>
            </tr>
        </thead>
        <tbody>
"""
    Path(ruta_html).write_text(html_head, encoding="utf-8")

def escribir_fila_reporte(ruta_html, archivo, regla_id, antes, despues):
    import html
    antes_esc = html.escape(antes)
    despues_esc = html.escape(despues)
    
    # Si el 'antes' y el 'despues' son muy parecidos, el reporte es confuso.
    # Aquí forzamos que se vea la fila completa de la tabla.
    fila = f"""
            <tr>
                <td><strong>{archivo}</strong></td>
                <td><small>{regla_id}</small></td>
                <td class="diff-del"><pre class="code">{antes_esc}</pre></td>
                <td class="diff-add"><pre class="code">{despues_esc}</pre></td>
            </tr>"""
    with open(ruta_html, "a", encoding="utf-8") as f:
        f.write(fila)

def finalizar_reporte_html(ruta_html):
    with open(ruta_html, "a", encoding="utf-8") as f:
        f.write("\n        </tbody>\n    </table>\n</body>\n</html>")

# --- FUNCIONES DE PROCESAMIENTO ---

def procesar_xml(path, reglas, ruta_html, dry_run):
    try:
        parser = etree.XMLParser(remove_blank_text=False)
        tree = etree.parse(str(path), parser)
        root = tree.getroot()
        ns = {"mvn": root.nsmap.get(None)} if root.nsmap.get(None) else {}
        cambiado = False

        for regla in reglas:
            pattern = regla.get("Target_Pattern")
            xpath = "//mvn:dependency" if ns else "//dependency"
            for dep in root.xpath(xpath, namespaces=ns):
                art_id = dep.find("mvn:artifactId", ns) if ns else dep.find("artifactId")
                
                if art_id is not None and fnmatch.fnmatch(art_id.text.strip(), pattern):
                    # Capturamos el bloque XML completo
                    antes_xml = etree.tostring(dep, encoding='unicode', pretty_print=True).strip()
                    
                    # Aplicamos los cambios del New_Block
                    for tag, nuevo_val in regla["New_Block"].items():
                        nodo = dep.find(f"mvn:{tag}", ns) if ns else dep.find(tag)
                        if nodo is not None:
                            nodo.text = nuevo_val
                    
                    despues_xml = etree.tostring(dep, encoding='unicode', pretty_print=True).strip()
                    
                    # Al usar html.escape en escribir_fila_reporte, ahora se verán los tags
                    escribir_fila_reporte(ruta_html, path.name, regla["ID"], antes_xml, despues_xml)
                    cambiado = True

        if cambiado and not dry_run:
            tree.write(str(path), encoding="utf-8", xml_declaration=True)
        return cambiado
    except Exception as e:
        print(f"Error XML: {e}")
        return False

def procesar_regex(path, reglas, ruta_html, dry_run):
    try:
        lineas = path.read_text(encoding="utf-8").splitlines()
        hubo_cambio = False
        for regla in reglas:
            patron = re.compile(regla["Oldval"], flags=re.IGNORECASE)
            nuevo_fmt = re.sub(r'\$(\d+)', r'\\g<\1>', regla["Newval"])
            nuevas_lineas = []
            for linea in lineas:
                if patron.search(linea):
                    antes = linea.strip()
                    despues = patron.sub(nuevo_fmt, linea).strip()
                    escribir_fila_reporte(ruta_html, path.name, regla["ID"], antes, despues)
                    nuevas_lineas.append(patron.sub(nuevo_fmt, linea))
                    hubo_cambio = True
                else:
                    nuevas_lineas.append(linea)
            lineas = nuevas_lineas
        if hubo_cambio and not dry_run:
            path.write_text("\n".join(lineas), encoding="utf-8")
        return hubo_cambio
    except Exception as e:
        print(f"Error Regex: {e}")
        return False

# --- PUNTO DE ENTRADA ---

def apply_replacements_in_directory(**kwargs):
    base_dir = Path(kwargs.get('base_dir'))
    config_file = Path(kwargs.get('config_file'))
    dry_run = kwargs.get('dry_run', False)
    ruta_html = kwargs.get('report_html') or (base_dir / "reporte.html")

    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)

    iniciar_reporte_html(ruta_html, base_dir, dry_run)
    
    search_files = config["Scan Options"]["Search_files"]
    xml_rules = config.get("XML Migration Rules", {})
    regex_rules = config.get("Regex Migration Rules", {})

    for path in base_dir.rglob("*"):
        if not path.is_file() or not any(fnmatch.fnmatch(path.name, p) for p in search_files):
            continue
        
        print(f"📂 Procesando: {path.name}")
        if path.name in xml_rules:
            procesar_xml(path, xml_rules[path.name], ruta_html, dry_run)
        
        for pattern, reglas in regex_rules.items():
            if fnmatch.fnmatch(path.name, pattern):
                procesar_regex(path, reglas, ruta_html, dry_run)

    finalizar_reporte_html(ruta_html)
    print(f"✨ Reporte finalizado en: {ruta_html}")