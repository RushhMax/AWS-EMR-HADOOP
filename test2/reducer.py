#!/usr/bin/env python3
"""
reducer.py — Índice Invertido con Hadoop MapReduce (AWS EMR)

Input:  pares ordenados  <término>\t<doc_id>  (ordenados por Hadoop shuffle)

Output: una línea por término:
        <término>\t<doc1>,<doc2>,...\t<frecuencia_total>

Formato de salida (3 columnas separadas por tabulación):
    término    doc1,doc2,...    N

Características:
  - Elimina documentos duplicados por término
  - Ordena documentos alfabéticamente
  - Emite frecuencia de documentos (cuántos docs contienen el término)
  - Filtra términos que aparecen en solo 1 doc (opcional, ver MIN_DOCS)
"""

import sys
from itertools import groupby

# Mínimo de documentos para incluir un término en el índice.
# Cambiar a 1 para incluir todos los términos.
MIN_DOCS = 1

def parse_stdin():
    """Genera tuplas (término, doc_id) desde stdin."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        term, doc = parts
        yield term.strip(), doc.strip()

def main():
    for term, pairs in groupby(parse_stdin(), key=lambda x: x[0]):
        docs = sorted(set(doc for _, doc in pairs))
        freq = len(docs)
        if freq < MIN_DOCS:
            continue
        doc_list = ",".join(docs)
        # Salida: término \t lista_docs \t cantidad_docs
        print(f"{term}\t{doc_list}\t{freq}")

if __name__ == "__main__":
    main()
