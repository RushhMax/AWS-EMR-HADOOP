#!/usr/bin/env python3
"""
Reducer - Índice Invertido con Hadoop MapReduce (AWS EMR)

Input:  pares ordenados  <palabra>\t<nombre_doc>   (ya ordenados por Hadoop)

Output: una línea por palabra:
        <palabra>\t<doc1>,<doc2>,...

Ejemplo de salida:
    hadoop  doc1.txt,doc2.txt
    index   doc1.txt
    python  doc1.txt,doc2.txt,doc3.txt

Notas:
  - Hadoop Streaming garantiza que los pares llegan ordenados por clave.
  - Se eliminan documentos duplicados para la misma palabra.
  - Los documentos se emiten en orden alfabético.
"""

import sys
from itertools import groupby


def parse_stdin():
    """Genera tuplas (palabra, doc_name) desde stdin."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        word, doc = parts
        yield word.strip(), doc.strip()


def main():
    # groupby funciona sobre datos YA ORDENADOS (Hadoop los ordena antes del reduce)
    for word, pairs in groupby(parse_stdin(), key=lambda x: x[0]):
        # Recolectar documentos únicos y ordenados
        docs = sorted(set(doc for _, doc in pairs))

        # Emitir  palabra → lista de docs separada por comas
        print(f"{word}\t{','.join(docs)}")


if __name__ == "__main__":
    main()
