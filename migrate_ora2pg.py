#!/usr/bin/env python3
"""
Script principal para invocar funtion.apply_replacements_in_directory
Añadida opción --report-html para especificar ruta del informe HTML.
"""

import argparse
from pathlib import Path
from migrator_core import apply_replacements_in_directory


def main():
    parser = argparse.ArgumentParser(description="Migración Oracle -> PostgreSQL: aplicar reemplazos según config.json")
    parser.add_argument("--config", "-c", default="config.json", help="Ruta al config.json")
    parser.add_argument("--base", "-b", required=True, help="Directorio base del proyecto a procesar")
    parser.add_argument("--dry-run", action="store_true", help="No escribir cambios, solo mostrar")
    parser.add_argument("--no-backup", action="store_true", help="No crear backup antes de modificar")
    parser.add_argument("--backup-dir", help="Directorio donde crear backups (opcional)")
    parser.add_argument("--report", help="Ruta del fichero de reporte CSV (opcional)")
    parser.add_argument("--report-html", help="Ruta del fichero de reporte HTML (opcional)")

    args = parser.parse_args()

    config_file = args.config
    base_dir = args.base
    dry_run = args.dry_run
    make_backup = not args.no_backup
    backup_dir = args.backup_dir
    report_file = args.report
    report_html = args.report_html

    base_path = Path(base_dir)
    if not base_path.exists() or not base_path.is_dir():
        print(f"ERROR: base directory no existe o no es un directorio: {base_dir}")
        return

    print(f"Configuración: config={config_file}, base={base_dir}, dry_run={dry_run}, backup={make_backup}")
    apply_replacements_in_directory(
        config_file=config_file,
        base_dir=base_dir,
        dry_run=dry_run,
        make_backup=make_backup,
        backup_dir=backup_dir,
        report_file=report_file,
        report_html=report_html
    )



if __name__ == "__main__":
    main()
