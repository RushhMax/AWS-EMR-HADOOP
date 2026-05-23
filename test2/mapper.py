#!/usr/bin/env python3
"""
mapper.py — Índice Invertido con Hadoop MapReduce (AWS EMR)

Input:  líneas con formato  <doc_id>\t<contenido>
        (Hadoop Streaming pasa cada línea por stdin)

Output: pares clave-valor  <término>\t<doc_id>
        uno por línea, separados por tabulación

Características:
  - Normalización unicode (tildes, ñ, etc.)
  - Stop words en español e inglés
  - Stemming simplificado (sufijos comunes)
  - Longitud mínima de término: 3 caracteres
  - Emite cada (término, doc) solo una vez por documento
"""

import sys
import re
import os
import unicodedata

# ── Stop words bilingüe (español + inglés) ────────────────────────────────────
STOP_WORDS = {
    # inglés
    "the","a","an","is","it","in","of","and","or","to","be","as","at","by",
    "for","on","with","from","that","this","was","are","has","have","had",
    "not","but","they","their","them","its","also","been","more","into",
    "than","can","will","which","when","where","who","how","all","any",
    "one","two","three","may","use","used","using","such","these","those",
    "each","both","very","well","just","only","over","after","before","out",
    "up","do","did","does","if","so","no","we","he","she","our","your",
    "his","her","my","me","us","him","you","i","am","would","could","should",
    # español
    "el","la","los","las","un","una","unos","unas","de","del","en","que",
    "por","con","se","es","al","su","pero","como","más","muy","ya","si",
    "entre","sobre","hasta","desde","hacia","durante","mediante","sin","bajo",
    "para","porque","aunque","cuando","donde","mientras","este","esta","estos",
    "estas","ese","esa","esos","esas","aquel","aquella","aquellos","aquellas",
    "mi","tu","nos","vos","les","lo","le","me","te","hay","era","fue",
    "son","ser","estar","han","sido","tiene","tienen","hacer","puede","pueden",
}

# ── Stemming simplificado ─────────────────────────────────────────────────────
SUFFIXES = [
    "aciones","ización","imientos","amiento","imiento","aciones",
    "amente","mente","ación","idades","idades","istas","ismo",
    "ables","ibles","ivos","ivas","ados","idas","ando","iendo",
    "ores","oras","ción","dad","ing","tion","tions","ings",
    "ers","ness","less","ful","ous","ive","ize","ized","izes",
    "tion","sion","ous","al","es","ed","er","ly","en",
]

def remove_accents(text: str) -> str:
    """Elimina tildes pero conserva ñ."""
    result = []
    for ch in unicodedata.normalize("NFD", text):
        cat = unicodedata.category(ch)
        if cat == "Mn":         # marca diacrítica
            continue
        result.append(ch)
    return "".join(result)

def normalize(word: str) -> str:
    """Limpia, pasa a minúsculas, elimina tildes y caracteres no alfanuméricos."""
    word = word.lower()
    word = remove_accents(word)
    word = re.sub(r"[^a-z0-9ñ]", "", word)
    return word

def stem(word: str) -> str:
    """Aplica stemming simplificado eliminando sufijos comunes."""
    if len(word) <= 5:
        return word
    for suffix in SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[: -len(suffix)]
    return word

def get_doc_name() -> str:
    """
    Obtiene el nombre del doc desde la variable de entorno de Hadoop.
    Si no existe (prueba local con formato doc\tcontenido) devuelve 'unknown'.
    """
    filepath = os.environ.get("MAP_INPUT_FILE", "")
    return os.path.basename(filepath) if filepath else "unknown"

def main():
    doc_name = get_doc_name()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Formato esperado: doc_id\tcontenido
        if "\t" in line:
            parts = line.split("\t", 1)
            doc_name = parts[0].strip() or doc_name
            content  = parts[1]
        else:
            content = line

        seen = set()
        for raw_word in content.split():
            token = normalize(raw_word)
            if not token or len(token) < 3:
                continue
            if token in STOP_WORDS:
                continue
            stemmed = stem(token)
            if not stemmed or len(stemmed) < 3:
                continue
            if stemmed not in seen:
                print(f"{stemmed}\t{doc_name}")
                seen.add(stemmed)

if __name__ == "__main__":
    main()
