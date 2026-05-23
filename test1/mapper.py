#!/usr/bin/env python3
"""
Mapper - Índice Invertido con Hadoop MapReduce (AWS EMR)

Input:  líneas del archivo con formato  <nombre_doc>\t<contenido>
        (Hadoop Streaming pasa cada línea por stdin)

Output: pares clave-valor  <palabra>\t<nombre_doc>
        separados por tabulación, uno por línea

Ejemplo de salida:
    python  doc1.txt
    index   doc1.txt
    hadoop  doc2.txt
    python  doc2.txt
"""

import sys
import re
import os

# ── Palabras vacías (stop words) en español e inglés ──────────────────────────
STOP_WORDS = {
    "the","a","an","is","it","in","of","and","or","to","be","as",
    "at","by","for","on","with","from","that","this","was","are",
    "el","la","los","las","un","una","de","del","en","que","por",
    "con","se","es","al","su","una","pero","como","más","muy",
}

def normalize(word: str) -> str:
    """Elimina caracteres no alfanuméricos y convierte a minúsculas."""
    return re.sub(r"[^a-záéíóúüñ0-9]", "", word.lower())


def get_doc_name() -> str:
    """
    Obtiene el nombre del archivo que Hadoop está procesando.
    La variable MAP_INPUT_FILE la establece Hadoop Streaming.
    Si no existe (prueba local), usa 'unknown_doc'.
    """
    filepath = os.environ.get("MAP_INPUT_FILE", "unknown_doc")
    return os.path.basename(filepath)


def main():
    doc_name = get_doc_name()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Si el archivo viene en formato  "doc_id\tcontenido" lo separamos
        if "\t" in line:
            parts = line.split("\t", 1)
            doc_name = parts[0].strip() or doc_name
            content  = parts[1]
        else:
            content = line

        # Tokenizar y emitir pares  palabra → doc_name
        words = content.split()
        seen  = set()   # emitir cada (word, doc) solo una vez por línea

        for raw_word in words:
            word = normalize(raw_word)
            if not word or word in STOP_WORDS or len(word) < 2:
                continue
            if word not in seen:
                print(f"{word}\t{doc_name}")
                seen.add(word)


if __name__ == "__main__":
    main()
