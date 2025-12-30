
from fileinput import filename
import json
import re
import fnmatch
import html
import os
from pathlib import Path
from datetime import datetime
from lxml import etree

# --- FUNCIONES DE REPORTE ---
def iniciar_reporte_html(ruta_html, base_dir, dry_run):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    modo_alert = "⚠️ MODO DRY-RUN: No se han aplicado cambios reales." if dry_run else "✅ MODO EJECUCIÓN: Cambios aplicados."
    
    Path(ruta_html).parent.mkdir(parents=True, exist_ok=True)

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
        
        /* ESTILOS DE COLUMNA Y RUTA */
        .col-file {{ width: 20%; }} /* Un poco más ancha para acomodar la ruta */
        .col-rule {{ width: 15%; }}
        .col-content {{ width: 32%; }}
        
        /* Estilo para la ruta debajo del nombre */
        .path-info {{ 
            display: block; 
            font-size: 0.85em; 
            color: #7f8c8d; 
            margin-top: 4px; 
            font-weight: normal;
            word-break: break-all; /* Rompe rutas muy largas */
        }}
    </style>
</head>
<body>
    <h1>Informe de Migración Oracle a PostgreSQL</h1>
    <div class="summary"><strong>Fecha:</strong> {ts}<br><strong>Directorio Base:</strong> {base_dir}</div>
    <div class="alert">{modo_alert}</div>
    <table>
        <thead>
            <tr>
                <th class="col-file">Archivo / Ruta</th>
                <th class="col-rule">Regla / ID</th>
                <th class="col-content">Antes (Oracle)</th>
                <th class="col-content">Después (PostgreSQL)</th>
            </tr>
        </thead>
        <tbody>
"""
    Path(ruta_html).write_text(html_head, encoding="utf-8")

def escribir_fila_reporte(ruta_html, archivo, ruta, regla_id, antes, despues):
    import html
    antes_esc = html.escape(antes)
    despues_esc = html.escape(despues)
    ruta_esc = html.escape(ruta) # Buena práctica escapar también la ruta
    
    fila = f"""
            <tr>
                <td>
                    <strong>{archivo}</strong>
                    <span class="path-info">{ruta_esc}</span>
                </td>
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
                    escribir_fila_reporte(ruta_html, path.name, str(path.parent), regla["ID"], antes_xml, despues_xml)
                    cambiado = True

        if cambiado and not dry_run:
            tree.write(str(path), encoding="utf-8", xml_declaration=True)
        return cambiado
    except Exception as e:
        print(f"Error XML: {e}")
        return False

def procesar_regex(path, reglas, ruta_html, dry_run):
    try:
        content = path.read_text(encoding="utf-8")
        lineas = content.splitlines()
        hubo_cambio = False
        path_str = str(path).replace("\\", "/").lower()

        for regla in reglas:

            must_contain = regla.get("Path_Contains", [])
            if must_contain:
                # Si es string único lo convertimos a lista
                if isinstance(must_contain, str): must_contain = [must_contain]
                # Si NINGUNA de las palabras está en el path, saltamos esta regla
                if not any(keyword.lower() in path_str for keyword in must_contain):
                    continue 

            # 2. Chequeo de Exclusión (Path_Not_Contains)
            # Si la regla define "Path_Not_Contains", el archivo NO DEBE tener esas palabras.
            must_exclude = regla.get("Path_Not_Contains", [])
            if must_exclude:
                if isinstance(must_exclude, str): must_exclude = [must_exclude]
                # Si ALGUNA de las palabras está en el path, saltamos esta regla
                if any(keyword.lower() in path_str for keyword in must_exclude):
                    continue
            
            # --- FIN LÓGICA FILTRADO ---

            patron = re.compile(regla["Oldval"], flags=re.IGNORECASE)
            # Preparamos el reemplazo de grupos de captura ($1 -> \g<1>) para Python
            nuevo_fmt = re.sub(r'\$(\d+)', r'\\g<\1>', regla["Newval"])
            
            nuevas_lineas = []
            for linea in lineas:
                if patron.search(linea):
                    antes = linea.strip()
                    despues = patron.sub(nuevo_fmt, linea).strip()
                    
                    # Solo registramos si hubo un cambio real
                    if antes != despues:
                        escribir_fila_reporte(ruta_html, path.name, str(path.parent), regla["ID"], antes, despues)
                        nuevas_lineas.append(patron.sub(nuevo_fmt, linea))
                        hubo_cambio = True
                    else:
                        nuevas_lineas.append(linea)
                else:
                    nuevas_lineas.append(linea)
            
            # Actualizamos las líneas para la siguiente regla
            lineas = nuevas_lineas

        if hubo_cambio and not dry_run:
            path.write_text("\n".join(lineas), encoding="utf-8")
        return hubo_cambio

    except Exception as e:
        print(f"Error Regex en {path}: {e}")
        return False

def procesar_secrets_conf(path, config_app_name, ruta_html, dry_run):
    try:
        content = path.read_text(encoding="utf-8")
        lines = content.splitlines()
        new_lines = []
        hubo_cambio = False

        pg_app_name = config_app_name
        replacement_pwd = "${env.PG_DATASOURCE_PWD}"
        exception_pattern = r"<.*-db-schema-app-password>"
        
        oracle_pass_sub = "db-schema-app-password"
        postgres_pass_sub = "db-pg-schema-app-password"

        for line in lines:
            new_lines.append(line)
            
            if "oracle/" in line:
                parts = [p.strip() for p in line.split(',')]
                
                if len(parts) >= 4:
                    new_line_str = line.replace("oracle/", "postgres/")
                    new_parts = [p.strip() for p in new_line_str.split(',')]

                    filename_full = path.name
                    if ".conf" in new_parts[2]:
                        dir_path = os.path.dirname(new_parts[2])
                        new_parts[2] = f"{dir_path}/{filename_full}".replace("//", "/")

                    current_pwd = new_parts[3]
                    
                    if re.search(exception_pattern, current_pwd):
                        new_parts[3] = current_pwd.replace(oracle_pass_sub, postgres_pass_sub)
                    else:
                        new_parts[3] = replacement_pwd

                    final_line = ", ".join(new_parts)
                    new_lines.append(final_line)
                    
                    escribir_fila_reporte(ruta_html, path.name, str(path.parent), "Secrets_Migration", line, final_line)
                    hubo_cambio = True

        if hubo_cambio and not dry_run:
            path.write_text("\n".join(new_lines), encoding="utf-8")
        
        return hubo_cambio

    except Exception as e:
        print(f"Error procesando secrets.conf: {e}")
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
    
    # Configuraciones de escaneo
    scan_opts = config.get("Scan Options", {})
    search_files = scan_opts.get("Search_files", [])
    excluded_dirs = scan_opts.get("Excluded Directories", [])
    excluded_files = scan_opts.get("Excluded Files", [])
    app_name = kwargs.get('app_name', 'default-app-name')

    xml_rules = config.get("XML Migration Rules", {})
    regex_rules = config.get("Regex Migration Rules", {})
    catch_all_rules = regex_rules.get("*", [])
    specific_regex_rules = {k: v for k, v in regex_rules.items() if k != "*"}

    # CAMBIO PRINCIPAL: Usar os.walk para poder podar directorios
    for root, dirs, files in os.walk(base_dir):
        dirs[:] = [d for d in dirs if not any(fnmatch.fnmatch(d, pat) for pat in excluded_dirs)]

        for filename in files:
            if any(fnmatch.fnmatch(filename, pat) for pat in excluded_files):
                continue

            if not any(fnmatch.fnmatch(filename, pat) for pat in search_files):
                continue
            
            path = Path(root) / filename
            print(f"📂 Procesando: {path.name}")

            file_was_processed = False

            if filename == "secrets.conf":
                procesar_secrets_conf(path, app_name, ruta_html, dry_run)
                file_was_processed = True
            
            elif path.name in xml_rules:
                procesar_xml(path, xml_rules[path.name], ruta_html, dry_run)
                file_was_processed = True
            
            else:
                for pattern, reglas in specific_regex_rules.items():
                    if fnmatch.fnmatch(path.name, pattern):
                        reglas_activas = reglas # O copy.deepcopy(reglas) si modificas algo
                        procesar_regex(path, reglas_activas, ruta_html, dry_run)
                        file_was_processed = True
            
            if not file_was_processed and catch_all_rules:
                print(f"   🔎 Usando escáner genérico para: {filename}")
                procesar_regex(path, catch_all_rules, ruta_html, dry_run)

    finalizar_reporte_html(ruta_html)
    print(f"✨ Reporte finalizado en: {ruta_html}")


