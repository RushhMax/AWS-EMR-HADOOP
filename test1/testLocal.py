#!/usr/bin/env python3
"""
Simula el flujo:
  docs → Mapper → sort (simula shuffle de Hadoop) → Reducer → índice
Salida esperada (índice invertido):
    Cada término → lista de documentos donde aparece
"""

import subprocess
import os
import sys
from pathlib import Path

BASE = Path(__file__).parent

# ── Documentos de prueba ───────────────────────────────────────────────────────
SAMPLE_DOCS = {
    "doc1.txt": "Python is a programming language widely used in data science, web development, machine learning, automation, and distributed systems. Python supports MapReduce, Hadoop streaming, and big data analytics pipelines.",
    
    "doc2.txt": "Hadoop MapReduce is a distributed processing framework designed for large scale data processing. Hadoop stores files in HDFS and executes jobs across clusters of machines in parallel.",
    
    "doc3.txt": "AWS EMR runs Hadoop and Spark clusters in the cloud. Python works well with EMR, allowing engineers to automate ETL jobs, data ingestion, and machine learning tasks.",
    
    "doc4.txt": "Un índice invertido es una estructura de datos utilizada en motores de búsqueda. Relaciona palabras clave con documentos donde aparecen. Sistemas como Elasticsearch, Lucene y Solr utilizan índices invertidos para búsquedas rápidas.",
    
    "doc5.txt": "Big data processing with Python, Hadoop, and Spark on AWS EMR clusters is common in enterprise environments. Spark improves iterative computation performance over traditional MapReduce workloads.",
    
    "doc6.txt": "Apache Spark is a distributed analytics engine optimized for speed and scalability. Spark supports SQL queries, machine learning libraries, graph processing, and real time streaming applications.",
    
    "doc7.txt": "Elasticsearch is a search engine based on Apache Lucene. It provides full text search, indexing, distributed storage, and REST APIs for scalable search systems.",
    
    "doc8.txt": "Machine learning pipelines often combine Python, Spark, and cloud services. Engineers train models using distributed datasets stored in Hadoop clusters or S3.",
    
    "doc9.txt": "Los motores de búsqueda modernos utilizan técnicas de tokenización, stemming, ranking y recuperación de información para encontrar documentos relevantes.",
    
    "doc10.txt": "AWS provides services such as EC2, S3, Lambda, DynamoDB, and EMR for scalable cloud computing. Many big data architectures rely on these services.",
    
    "doc11.txt": "Data engineers process terabytes of information daily. ETL pipelines extract, transform, and load information into distributed databases and data warehouses.",
    
    "doc12.txt": "Spark Streaming enables real time analytics on continuous streams of data. Applications include fraud detection, monitoring systems, and IoT platforms.",
    
    "doc13.txt": "Information retrieval systems use ranking algorithms such as TF-IDF and BM25 to determine document relevance in search results.",
    
    "doc14.txt": "Hadoop Distributed File System or HDFS stores large datasets across many nodes. Replication improves fault tolerance and reliability in distributed environments.",
    
    "doc15.txt": "Python libraries like pandas, numpy, scikit-learn, and pyspark are popular tools in data science and big data ecosystems.",
    
    "doc16.txt": "Un sistema de recuperación de información puede indexar millones de documentos utilizando estructuras eficientes como tablas hash y árboles B.",
    
    "doc17.txt": "Cloud computing platforms allow organizations to dynamically scale resources. AWS EMR simplifies cluster management for Hadoop and Spark applications.",
    
    "doc18.txt": "Search engines crawl web pages, extract content, and create indexes that allow users to quickly retrieve relevant information from billions of documents.",
    
    "doc19.txt": "Apache Lucene is a high performance text search library written in Java. It powers search technologies such as Solr and Elasticsearch.",
    
    "doc20.txt": "Big data systems must handle scalability, consistency, availability, and fault tolerance. Distributed computing frameworks solve these challenges efficiently."
}

def create_sample_input(path: Path) -> Path:
    """Crea el archivo de input con formato  doc_name\tcontenido."""
    input_file = path / "input.txt"
    with open(input_file, "w") as f:
        for doc_name, content in SAMPLE_DOCS.items():
            f.write(f"{doc_name}\t{content}\n")
    print(f"✓ Input creado: {input_file}")
    return input_file


def run_pipeline(input_file: Path) -> str:
    """
    Simula el pipeline Hadoop Streaming:
        cat input | mapper | sort | reducer
    """
    mapper_path  = BASE / "mapper.py"
    reducer_path = BASE / "reducer.py"

    # Paso 1: Mapper
    map_proc = subprocess.run(
        [sys.executable, str(mapper_path)],
        stdin=open(input_file),
        capture_output=True, text=True
    )
    if map_proc.returncode != 0:
        print("ERROR en Mapper:\n", map_proc.stderr)
        sys.exit(1)

    mapper_output = map_proc.stdout

    # Paso 2: Sort (simula el shuffle de Hadoop)
    sorted_lines = sorted(mapper_output.strip().split("\n"))
    sorted_input = "\n".join(sorted_lines)

    # Paso 3: Reducer
    reduce_proc = subprocess.run(
        [sys.executable, str(reducer_path)],
        input=sorted_input,
        capture_output=True, text=True
    )
    if reduce_proc.returncode != 0:
        print("ERROR en Reducer:\n", reduce_proc.stderr)
        sys.exit(1)

    return reduce_proc.stdout


def print_index(index_text: str):
    """Imprime el índice invertido de forma legible."""
    print("\n" + "═" * 60)
    print("  ÍNDICE INVERTIDO  (término → documentos)")
    print("═" * 60)
    lines = sorted(index_text.strip().split("\n"))
    for line in lines:
        if "\t" in line:
            word, docs = line.split("\t", 1)
            doc_list = docs.split(",")
            print(f"  {word:<20} → {', '.join(doc_list)}")
    print("═" * 60)
    print(f"\n  Total de términos indexados: {len(lines)}")


def main():
    tmp_dir = BASE / "tmp"
    tmp_dir.mkdir(exist_ok=True)

    print("\n  Índice Invertido con MapReduce  (prueba local)\n")

    # Mostrar documentos de muestra
    print("Documentos de entrada:")
    for name, content in SAMPLE_DOCS.items():
        preview = content[:60] + ("..." if len(content) > 60 else "")
        print(f"  {name}: {preview}")

    # Crear input y correr pipeline
    input_file = create_sample_input(tmp_dir)
    index_text = run_pipeline(input_file)

    # Guardar output
    output_file = tmp_dir / "index_output.txt"
    output_file.write_text(index_text)
    print(f" Output guardado: {output_file}")

    # Mostrar índice
    print_index(index_text)

    # Ejemplo de consulta simple
    # print("\n  Ejemplo: buscar documentos que contienen 'python'")
    # for line in index_text.strip().split("\n"):
    #     if line.startswith("python\t"):
    #         _, docs = line.split("\t", 1)
    #         print(f"  'python' aparece en: {docs}")
    #         break


if __name__ == "__main__":
    main()

