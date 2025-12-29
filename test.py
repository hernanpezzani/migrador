# test_pom_replace_fixed.py
import re
from pathlib import Path

pom_path = Path(r"C:\Users\hernan\GIT\Github\migrador\test\hex-oracle-app\pom.xml")
pom = pom_path.read_text(encoding="utf-8")

pat = re.compile(
    r"(<dependency\b[^>]*>.*?<groupId>\s*com\.oracle\.database\.jdbc\s*</groupId>.*?"
    r"<artifactId>\s*ojdbc[0-9]*\s*</artifactId>.*?<version>)(.*?)(</version>.*?</dependency>)",
    flags=re.MULTILINE | re.DOTALL
)

m = pat.search(pom)
if m:
    print("Encontrado bloque dependency (original):\n")
    # imprimir el bloque original completo
    start_orig = m.start()
    end_orig = m.end()
    print(pom[start_orig:end_orig])
    print("\nPropuesto (fragmento modificado):\n")
    # aplicar la sustitución sobre todo el contenido y extraer el bloque modificado
    replaced = pat.sub(r"\1 42.7.3\3", pom, count=1)
    # buscar el mismo bloque en el texto reemplazado usando la nueva groupId/artifactId
    pat_after = re.compile(r"<dependency\b[^>]*>.*?<groupId>\s*org\.postgresql\s*</groupId>.*?<artifactId>\s*postgresql\s*</artifactId>.*?</dependency>", flags=re.MULTILINE | re.DOTALL)
    m2 = pat_after.search(replaced)
    if m2:
        print(replaced[m2.start():m2.end()])
    else:
        # fallback: mostrar la región alrededor de la posición original
        start = max(0, start_orig - 200)
        end = min(len(replaced), end_orig + 200)
        print(replaced[start:end])
else:
    print("No se encontró el bloque dependency con groupId com.oracle.database.jdbc y artifactId ojdbc*")
