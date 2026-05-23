#!/usr/bin/env python3
"""
generate_input.py — Genera un corpus grande y realista para el índice invertido.

Produce un archivo input.txt con 500 documentos (~150KB) sobre los dominios:
  - Big data y distributed computing
  - Machine learning e IA
  - Cloud computing (AWS, GCP, Azure)
  - Bases de datos y sistemas de búsqueda
  - Ciencia de datos y estadística
  - Ingeniería de software
  - Ciberseguridad
  - Fintech y economía digital
  - Biotecnología e informática médica
  - IoT y sistemas embebidos

Uso:
    python generate_input.py [--docs N] [--output PATH]
    python generate_input.py --docs 1000 --output input_large.txt
"""

import random
import argparse
from pathlib import Path
from itertools import product as iproduct

# ── Vocabulario temático por dominio ─────────────────────────────────────────

DOMAINS = {

"bigdata": {
    "nouns": [
        "Hadoop","MapReduce","Spark","HDFS","Hive","Pig","HBase","Kafka",
        "Flink","Storm","Samza","Zookeeper","Yarn","Oozie","Sqoop","Flume",
        "cluster","pipeline","batch","streaming","throughput","latency",
        "partition","replication","sharding","checkpoint","lineage","DAG",
        "executor","driver","shuffle","spill","combiner","partitioner",
        "namenode","datanode","tasktracker","jobtracker","container",
        "terabyte","petabyte","exabyte","dataset","ingestion","compaction",
    ],
    "verbs": [
        "processes","distributes","parallelizes","aggregates","replicates",
        "partitions","shuffles","sorts","merges","compresses","checkpoints",
        "schedules","allocates","monitors","optimizes","scales","ingests",
    ],
    "adjectives": [
        "distributed","fault-tolerant","scalable","parallel","append-only",
        "immutable","columnar","partitioned","replicated","compressed",
        "high-throughput","low-latency","resilient","elastic","durable",
    ],
    "templates": [
        "{noun1} {verb} large-scale {noun2} across hundreds of nodes in a {adj} cluster.",
        "The {adj} architecture of {noun1} enables {verb} operations on {noun2} efficiently.",
        "{noun1} uses {noun2} to {verb} data with {adj} guarantees across the cluster.",
        "Engineers deploy {noun1} to {verb} {noun2} workloads in a {adj} environment.",
        "A {adj} {noun1} pipeline {verb} {noun2} at petabyte scale with minimal overhead.",
        "{noun2} {verb} through {noun1} using {adj} techniques for maximum performance.",
        "The {noun1} framework {verb} {noun2} by leveraging {adj} distributed processing.",
        "Production {noun1} systems {verb} {noun2} while maintaining {adj} data guarantees.",
    ],
},

"ml": {
    "nouns": [
        "neural network","transformer","embedding","attention","gradient",
        "backpropagation","optimizer","loss function","regularization","dropout",
        "batch normalization","convolution","pooling","activation","softmax",
        "tokenizer","vocabulary","fine-tuning","pretraining","inference",
        "hyperparameter","epoch","learning rate","weight decay","momentum",
        "BERT","GPT","LLM","diffusion model","GAN","autoencoder","VAE",
        "feature extraction","transfer learning","zero-shot","few-shot",
        "reinforcement learning","reward","policy","agent","environment",
        "cross-entropy","precision","recall","F1-score","AUC-ROC",
    ],
    "verbs": [
        "trains","infers","predicts","classifies","clusters","embeds",
        "fine-tunes","optimizes","regularizes","generalizes","overfits",
        "converges","backpropagates","samples","generates","encodes","decodes",
    ],
    "adjectives": [
        "supervised","unsupervised","self-supervised","autoregressive",
        "pre-trained","fine-tuned","stochastic","deterministic","sparse",
        "dense","quantized","pruned","distilled","multi-modal","causal",
    ],
    "templates": [
        "The {adj} {noun1} {verb} on large corpora to learn {noun2} representations.",
        "Researchers use {noun1} to {verb} {noun2} with {adj} objectives.",
        "{noun1} {verb} {noun2} by applying {adj} techniques during training.",
        "A {adj} {noun1} model {verb} {noun2} achieving state-of-the-art results.",
        "The {noun2} mechanism allows {noun1} to {verb} long-range dependencies {adj}ly.",
        "{noun1} and {noun2} combine to {verb} {adj} representations at scale.",
        "Training {noun1} requires careful tuning of {noun2} in {adj} settings.",
        "Modern {adj} {noun1} systems {verb} {noun2} using billions of parameters.",
    ],
},

"cloud": {
    "nouns": [
        "AWS","EC2","S3","Lambda","DynamoDB","RDS","EMR","Glue","Athena",
        "CloudFormation","Terraform","Kubernetes","Docker","ECS","EKS",
        "VPC","subnet","security group","load balancer","auto scaling",
        "GCP","BigQuery","Dataflow","Pub/Sub","Cloud Run","Firestore",
        "Azure","Blob Storage","CosmosDB","HDInsight","Databricks",
        "microservice","container","orchestration","serverless","IaC",
        "multi-cloud","hybrid cloud","edge computing","CDN","WAF",
        "SLA","availability zone","region","replication","failover",
    ],
    "verbs": [
        "deploys","provisions","orchestrates","autoscales","monitors",
        "replicates","migrates","containerizes","abstracts","manages",
        "routes","balances","encrypts","authenticates","authorizes",
    ],
    "adjectives": [
        "managed","serverless","multi-region","highly-available","elastic",
        "on-demand","pay-per-use","fault-tolerant","geo-redundant","encrypted",
    ],
    "templates": [
        "{noun1} {verb} {adj} workloads across multiple {noun2} regions.",
        "The {adj} {noun1} service {verb} {noun2} without manual intervention.",
        "Teams use {noun1} to {verb} {noun2} with {adj} infrastructure as code.",
        "A {adj} {noun1} architecture {verb} {noun2} for enterprise workloads.",
        "{noun1} {verb} {noun2} automatically using {adj} scaling policies.",
        "Engineers configure {noun1} to {verb} {noun2} in a {adj} manner.",
        "The {adj} {noun1} platform {verb} {noun2} across availability zones.",
        "{noun2} {verb} on {noun1} providing {adj} performance guarantees.",
    ],
},

"databases": {
    "nouns": [
        "PostgreSQL","MySQL","MongoDB","Cassandra","Redis","Elasticsearch",
        "Neo4j","ClickHouse","Snowflake","BigQuery","Redshift","Pinecone",
        "index","B-tree","LSM-tree","WAL","MVCC","transaction","ACID",
        "BASE","eventual consistency","quorum","consensus","Raft","Paxos",
        "inverted index","TF-IDF","BM25","vector search","embedding store",
        "schema","migration","shard","replica","primary key","foreign key",
        "query planner","execution plan","join","aggregation","window function",
        "materialized view","CDC","replication log","binlog","WAL",
    ],
    "verbs": [
        "indexes","queries","replicates","partitions","compacts","vacuums",
        "checkpoints","recovers","serializes","deserializes","caches",
        "invalidates","flushes","commits","rollbacks","merges",
    ],
    "adjectives": [
        "relational","document-oriented","graph-based","key-value","columnar",
        "time-series","vector","in-memory","persistent","append-only",
        "strongly-consistent","eventually-consistent","ACID-compliant","sharded",
    ],
    "templates": [
        "{noun1} {verb} {noun2} using {adj} storage structures for fast retrieval.",
        "The {adj} {noun1} engine {verb} {noun2} with low read latency.",
        "{noun1} {verb} {adj} {noun2} across multiple nodes for high availability.",
        "A {adj} {noun1} system {verb} {noun2} to support complex analytical queries.",
        "{noun2} {verb} through {noun1} maintaining {adj} consistency guarantees.",
        "Database engineers use {noun1} to {verb} {noun2} in {adj} workloads.",
        "The {adj} {noun1} stores {noun2} optimized for analytical access patterns.",
        "Modern {noun1} systems {verb} {noun2} using {adj} indexing strategies.",
    ],
},

"datasci": {
    "nouns": [
        "pandas","numpy","scikit-learn","matplotlib","seaborn","plotly",
        "Jupyter","statistics","regression","classification","clustering",
        "feature engineering","dimensionality reduction","PCA","t-SNE","UMAP",
        "cross-validation","hyperparameter tuning","grid search","pipeline",
        "data cleaning","imputation","normalization","standardization",
        "correlation","covariance","hypothesis testing","p-value","confidence interval",
        "A/B testing","causal inference","time series","seasonality","trend",
        "anomaly detection","outlier","distribution","probability","Bayesian",
    ],
    "verbs": [
        "analyzes","visualizes","models","predicts","clusters","transforms",
        "imputes","normalizes","validates","tunes","evaluates","interprets",
        "bootstraps","samples","encodes","decodes","aggregates",
    ],
    "adjectives": [
        "exploratory","predictive","descriptive","inferential","supervised",
        "unsupervised","statistical","probabilistic","robust","unbiased",
    ],
    "templates": [
        "Data scientists use {noun1} to {verb} {noun2} in {adj} workflows.",
        "The {adj} {noun1} library {verb} {noun2} for statistical analysis.",
        "{noun1} {verb} {noun2} revealing {adj} patterns in large datasets.",
        "A {adj} {noun1} approach {verb} {noun2} with interpretable results.",
        "{noun2} {verb} using {noun1} in {adj} data science pipelines.",
        "Teams {verb} {noun2} with {noun1} applying {adj} statistical methods.",
        "The {adj} {noun1} model {verb} {noun2} achieving strong generalization.",
        "{noun1} enables analysts to {verb} {noun2} in a {adj} manner.",
    ],
},

"security": {
    "nouns": [
        "encryption","TLS","PKI","certificate","OAuth","JWT","RBAC","ABAC",
        "zero-trust","firewall","IDS","IPS","SIEM","SOC","CVE","exploit",
        "vulnerability","penetration testing","red team","blue team",
        "ransomware","phishing","SQL injection","XSS","CSRF","buffer overflow",
        "hashing","salting","bcrypt","Argon2","AES","RSA","ECDSA","Ed25519",
        "MFA","SSO","LDAP","Active Directory","IAM","secret manager",
        "audit log","compliance","GDPR","SOC2","ISO27001","NIST framework",
    ],
    "verbs": [
        "encrypts","authenticates","authorizes","audits","scans","patches",
        "monitors","detects","mitigates","hardens","rotates","revokes",
        "signs","verifies","hashes","tokenizes","obfuscates",
    ],
    "adjectives": [
        "end-to-end encrypted","zero-knowledge","least-privilege","defense-in-depth",
        "threat-modeled","compliant","hardened","immutable","auditable","air-gapped",
    ],
    "templates": [
        "The {adj} {noun1} system {verb} {noun2} before granting access.",
        "{noun1} {verb} {noun2} using {adj} cryptographic protocols.",
        "Security teams use {noun1} to {verb} {noun2} in {adj} environments.",
        "A {adj} {noun1} implementation {verb} {noun2} against known attack vectors.",
        "{noun2} {verb} through {noun1} following {adj} security standards.",
        "Engineers configure {noun1} to {verb} {noun2} with {adj} controls.",
        "The {adj} {noun1} policy {verb} {noun2} across the organization.",
        "Modern {noun1} frameworks {verb} {noun2} using {adj} best practices.",
    ],
},

"fintech": {
    "nouns": [
        "blockchain","smart contract","DeFi","NFT","cryptocurrency","Bitcoin",
        "Ethereum","Solidity","Web3","consensus mechanism","proof of work",
        "proof of stake","ledger","wallet","transaction","gas fee","liquidity",
        "trading algorithm","quantitative finance","risk model","portfolio",
        "derivatives","options","futures","arbitrage","market maker",
        "payment gateway","KYC","AML","RegTech","open banking","API banking",
        "credit scoring","fraud detection","chargeback","settlement","clearing",
    ],
    "verbs": [
        "validates","settles","clears","tokenizes","mints","burns","stakes",
        "liquidates","hedges","arbitrages","scores","detects","reconciles",
        "audits","complies","processes","transfers",
    ],
    "adjectives": [
        "decentralized","permissioned","immutable","trustless","programmable",
        "atomic","transparent","pseudonymous","regulatory-compliant","real-time",
    ],
    "templates": [
        "The {adj} {noun1} protocol {verb} {noun2} without intermediaries.",
        "{noun1} {verb} {noun2} using {adj} cryptographic proofs.",
        "Financial institutions use {noun1} to {verb} {noun2} {adj}ly.",
        "A {adj} {noun1} system {verb} {noun2} with full auditability.",
        "{noun2} {verb} through {noun1} leveraging {adj} consensus mechanisms.",
        "Engineers deploy {noun1} to {verb} {noun2} in {adj} financial systems.",
        "The {adj} {noun1} network {verb} {noun2} across global participants.",
        "Modern {noun1} platforms {verb} {noun2} using {adj} smart contracts.",
    ],
},

"bioinfo": {
    "nouns": [
        "genome","DNA","RNA","protein","sequence alignment","BLAST","FASTA",
        "variant calling","SNP","CRISPR","gene expression","transcriptomics",
        "proteomics","metabolomics","phylogenetics","metagenomics","assembly",
        "annotation","pipeline","reference genome","read mapping","coverage",
        "clinical trial","EHR","FHIR","HL7","ICD-10","diagnosis","prognosis",
        "biomarker","drug discovery","molecular docking","simulation",
        "population genetics","allele frequency","haplotype","linkage disequilibrium",
    ],
    "verbs": [
        "sequences","aligns","assembles","annotates","predicts","models",
        "simulates","clusters","classifies","identifies","validates","quantifies",
        "normalizes","imputes","corrects","filters","maps",
    ],
    "adjectives": [
        "high-throughput","next-generation","single-cell","multi-omics","clinical",
        "genomic","proteomic","structural","functional","comparative","statistical",
    ],
    "templates": [
        "Researchers use {noun1} to {verb} {noun2} from {adj} sequencing data.",
        "The {adj} {noun1} pipeline {verb} {noun2} with high accuracy.",
        "{noun1} {verb} {noun2} enabling {adj} insights into disease mechanisms.",
        "A {adj} {noun1} approach {verb} {noun2} across thousands of samples.",
        "{noun2} {verb} through {noun1} using {adj} computational methods.",
        "Bioinformaticians {verb} {noun2} with {noun1} in {adj} workflows.",
        "The {adj} {noun1} tool {verb} {noun2} at population scale.",
        "Modern {noun1} methods {verb} {noun2} combining {adj} data sources.",
    ],
},

"iot": {
    "nouns": [
        "sensor","actuator","MQTT","CoAP","edge device","gateway","firmware",
        "microcontroller","Arduino","Raspberry Pi","ESP32","RTOS","interrupt",
        "telemetry","time series","InfluxDB","Grafana","ThingsBoard","AWS IoT",
        "protocol","Zigbee","LoRa","BLE","WiFi","NB-IoT","LTE-M","5G",
        "digital twin","predictive maintenance","anomaly detection","OTA update",
        "energy harvesting","sleep mode","duty cycle","power budget","latency",
        "fleet management","provisioning","device shadow","message broker","QoS",
    ],
    "verbs": [
        "collects","transmits","aggregates","monitors","controls","provisions",
        "updates","detects","triggers","processes","stores","visualizes",
        "calibrates","synchronizes","compresses","encrypts","routes",
    ],
    "adjectives": [
        "low-power","real-time","edge-native","constrained","heterogeneous",
        "bidirectional","event-driven","embedded","wireless","mesh-networked",
    ],
    "templates": [
        "The {adj} {noun1} {verb} {noun2} every few milliseconds to the cloud.",
        "{noun1} {verb} {noun2} using {adj} protocols for reliable delivery.",
        "IoT engineers use {noun1} to {verb} {noun2} in {adj} deployments.",
        "A {adj} {noun1} system {verb} {noun2} with minimal power consumption.",
        "{noun2} {verb} through {noun1} leveraging {adj} edge processing.",
        "Devices {verb} {noun2} via {noun1} in a {adj} network topology.",
        "The {adj} {noun1} firmware {verb} {noun2} with deterministic timing.",
        "Modern {noun1} platforms {verb} {noun2} using {adj} communication stacks.",
    ],
},

"softeng": {
    "nouns": [
        "API","REST","GraphQL","gRPC","microservice","monolith","event sourcing",
        "CQRS","domain driven design","bounded context","saga","outbox pattern",
        "CI/CD","GitHub Actions","Jenkins","ArgoCD","Helm","Ansible","Chef",
        "observability","tracing","metrics","logging","OpenTelemetry","Prometheus",
        "design pattern","SOLID","DRY","KISS","refactoring","technical debt",
        "code review","pull request","branching strategy","semantic versioning",
        "unit test","integration test","end-to-end test","TDD","BDD","mocking",
    ],
    "verbs": [
        "designs","implements","refactors","tests","deploys","monitors",
        "documents","reviews","integrates","abstracts","decouples","scales",
        "versions","releases","ships","validates","benchmarks",
    ],
    "adjectives": [
        "idempotent","stateless","loosely-coupled","highly-cohesive","testable",
        "observable","deployable","maintainable","extensible","backward-compatible",
    ],
    "templates": [
        "Engineers use {noun1} to {verb} {noun2} in {adj} system architectures.",
        "The {adj} {noun1} pattern {verb} {noun2} without tight coupling.",
        "{noun1} {verb} {noun2} ensuring {adj} behavior across service boundaries.",
        "A {adj} {noun1} approach {verb} {noun2} with clear separation of concerns.",
        "{noun2} {verb} through {noun1} following {adj} software principles.",
        "Teams {verb} {noun2} using {noun1} in {adj} delivery pipelines.",
        "The {adj} {noun1} interface {verb} {noun2} across distributed systems.",
        "Modern {noun1} frameworks {verb} {noun2} with {adj} extensibility.",
    ],
},

}

DOMAIN_NAMES = list(DOMAINS.keys())

def pick(lst):
    return random.choice(lst)

def generate_sentence(domain_key: str) -> str:
    d = DOMAINS[domain_key]
    template = pick(d["templates"])
    return template.format(
        noun1 = pick(d["nouns"]),
        noun2 = pick(d["nouns"]),
        verb  = pick(d["verbs"]),
        adj   = pick(d["adjectives"]),
    )

def generate_document(doc_id: str, primary_domain: str, n_sentences: int) -> str:
    """Genera un documento con n_sentences oraciones mezclando dominios."""
    sentences = []
    # 60% del dominio principal, 40% de dominios relacionados
    for _ in range(n_sentences):
        if random.random() < 0.6:
            sentences.append(generate_sentence(primary_domain))
        else:
            sentences.append(generate_sentence(pick(DOMAIN_NAMES)))
    return " ".join(sentences)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs",   type=int, default=500,       help="Número de documentos")
    parser.add_argument("--output", type=str, default="input.txt", help="Archivo de salida")
    parser.add_argument("--seed",   type=int, default=42,         help="Semilla aleatoria")
    args = parser.parse_args()

    random.seed(args.seed)

    # Distribuir documentos entre dominios
    docs_per_domain = args.docs // len(DOMAIN_NAMES)
    remainder       = args.docs % len(DOMAIN_NAMES)

    doc_specs = []
    for i, domain in enumerate(DOMAIN_NAMES):
        count = docs_per_domain + (1 if i < remainder else 0)
        for j in range(count):
            doc_id = f"{domain}_doc{j+1:04d}.txt"
            n_sentences = random.randint(8, 20)   # entre 8 y 20 oraciones por doc
            doc_specs.append((doc_id, domain, n_sentences))

    random.shuffle(doc_specs)

    out_path = Path(args.output)
    total_chars = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for doc_id, domain, n_sentences in doc_specs:
            content = generate_document(doc_id, domain, n_sentences)
            line    = f"{doc_id}\t{content}\n"
            f.write(line)
            total_chars += len(line)

    total_kb = total_chars / 1024
    print(f"✓ Generados {len(doc_specs)} documentos → {out_path}")
    print(f"  Tamaño aproximado : {total_kb:.1f} KB")
    print(f"  Dominios cubiertos: {', '.join(DOMAIN_NAMES)}")
    print(f"  Formato           : doc_id<TAB>contenido (una línea por doc)")

if __name__ == "__main__":
    main()
