import argparse
from migration_core import apply_replacements_in_directory

def main():
    parser = argparse.ArgumentParser(description="Migrador Oracle a PostgreSQL")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--base", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-html", help="Ruta del informe HTML")

    args = parser.parse_args()

    apply_replacements_in_directory(
        config_file=args.config,
        base_dir=args.base,
        dry_run=args.dry_run,
        report_html=args.report_html
    )

if __name__ == "__main__":
    main()

    