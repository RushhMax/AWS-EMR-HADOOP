#!/usr/bin/env python3
"""
submit_emr.py  -  Lanza el job de índice invertido en AWS EMR
usando Hadoop Streaming.

Prerequisitos:
    pip install boto3
    aws configure   (o variables de entorno AWS_ACCESS_KEY_ID, etc.)

Uso:
    python submit_emr.py \
        --bucket    mi-bucket-s3 \
        --input     s3://mi-bucket-s3/input/ \
        --output    s3://mi-bucket-s3/output/inverted-index \
        --region    us-east-1

El script:
  1. Sube mapper.py y reducer.py a S3.
  2. Crea un cluster EMR (o usa uno existente con --cluster-id).
  3. Agrega el step de Hadoop Streaming.
  4. Espera a que termine e imprime el estado.
"""

import argparse
import boto3
import os
import sys
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).parent

# ── Configuración del cluster EMR ─────────────────────────────────────────────
EMR_RELEASE   = "emr-7.1.0"           # Versión de EMR con Hadoop 3.x
INSTANCE_TYPE = "m5.xlarge"           # Tipo de instancia EC2
NUM_WORKERS   = 2                     # Nodos worker (aumentar para más datos)
LOG_PREFIX    = "emr-logs"

# Ruta al JAR de Hadoop Streaming dentro de EMR
STREAMING_JAR = (
    "/usr/lib/hadoop/tools/lib/hadoop-streaming.jar"
)


def upload_scripts(s3_client, bucket: str, prefix: str) -> tuple[str, str]:
    """Sube mapper.py y reducer.py al bucket S3 y devuelve sus URIs."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_prefix = f"{prefix}/scripts/{ts}"

    for script_name in ["mapper.py", "reducer.py"]:
        local_path = BASE / script_name
        s3_key     = f"{s3_prefix}/{script_name}"
        print(f"  Subiendo {script_name} → s3://{bucket}/{s3_key}")
        s3_client.upload_file(str(local_path), bucket, s3_key)

    mapper_uri  = f"s3://{bucket}/{s3_prefix}/mapper.py"
    reducer_uri = f"s3://{bucket}/{s3_prefix}/reducer.py"
    return mapper_uri, reducer_uri


def create_cluster_and_run(
    emr_client,
    s3_client,
    bucket:     str,
    input_uri:  str,
    output_uri: str,
    cluster_id: str | None,
) -> str:
    """
    Crea un cluster EMR transient (se apaga al terminar) o usa uno existente,
    agrega el step de Hadoop Streaming y devuelve el step ID.
    """
    prefix = "inverted-index"
    mapper_uri, reducer_uri = upload_scripts(s3_client, bucket, prefix)

    # Step: ejecutar Hadoop Streaming
    step = {
        "Name": "InvertedIndex-MapReduce",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "hadoop-streaming",
                "-files",       f"{mapper_uri},{reducer_uri}",
                "-mapper",      "mapper.py",
                "-reducer",     "reducer.py",
                "-input",       input_uri,
                "-output",      output_uri,
                "-jobconf",     "mapreduce.job.reduces=4",          # nro. de reducers
                "-jobconf",     "mapreduce.map.memory.mb=2048",
                "-jobconf",     "mapreduce.reduce.memory.mb=4096",
            ],
        },
    }

    if cluster_id:
        # Agregar step a cluster existente
        print(f"\n  Agregando step al cluster existente {cluster_id}...")
        resp = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step],
        )
        return cluster_id, resp["StepIds"][0]

    # Crear cluster nuevo (transient: se apaga al terminar)
    print("\n  Creando cluster EMR...")
    resp = emr_client.run_job_flow(
        Name=f"inverted-index-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        ReleaseLabel=EMR_RELEASE,
        Applications=[{"Name": "Hadoop"}],
        Instances={
            "MasterInstanceType": INSTANCE_TYPE,
            "SlaveInstanceType":  INSTANCE_TYPE,
            "InstanceCount":      NUM_WORKERS + 1,  # +1 para el master
            "KeepJobFlowAliveWhenNoSteps": False,   # cluster transient
            "Ec2KeyName": os.environ.get("EC2_KEY_NAME", ""),
        },
        Steps=[step],
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{bucket}/{LOG_PREFIX}/",
        Tags=[{"Key": "Project", "Value": "inverted-index"}],
        VisibleToAllUsers=True,
    )
    cluster_id = resp["JobFlowId"]
    return cluster_id, None   # step id no disponible hasta que inicie


def wait_for_cluster(emr_client, cluster_id: str):
    """Espera a que el cluster termine e imprime progreso."""
    import time
    print(f"\n  Esperando al cluster {cluster_id}...")
    waiter = emr_client.get_waiter("cluster_terminated")
    try:
        waiter.wait(
            ClusterId=cluster_id,
            WaiterConfig={"Delay": 30, "MaxAttempts": 60},  # hasta 30 min
        )
    except Exception:
        pass  # El waiter puede lanzar excepción si el estado no es TERMINATED

    # Obtener estado final
    resp   = emr_client.describe_cluster(ClusterId=cluster_id)
    status = resp["Cluster"]["Status"]
    state  = status["State"]
    reason = status.get("StateChangeReason", {}).get("Message", "")
    print(f"\n  Estado final del cluster: {state}")
    if reason:
        print(f"  Motivo: {reason}")
    return state


def main():
    parser = argparse.ArgumentParser(description="Lanza índice invertido en AWS EMR")
    parser.add_argument("--bucket",     required=True,  help="Nombre del bucket S3 (sin s3://)")
    parser.add_argument("--input",      required=True,  help="URI S3 de los documentos de entrada")
    parser.add_argument("--output",     required=True,  help="URI S3 de salida del índice")
    parser.add_argument("--region",     default="us-east-1")
    parser.add_argument("--cluster-id", default=None,   help="ID de cluster EMR existente (opcional)")
    parser.add_argument("--no-wait",    action="store_true", help="No esperar al terminar")
    args = parser.parse_args()

    print("\n🚀 Índice Invertido → AWS EMR\n")
    print(f"  Bucket : {args.bucket}")
    print(f"  Input  : {args.input}")
    print(f"  Output : {args.output}")
    print(f"  Región : {args.region}")

    emr_client = boto3.client("emr", region_name=args.region)
    s3_client  = boto3.client("s3",  region_name=args.region)

    cluster_id, step_id = create_cluster_and_run(
        emr_client, s3_client,
        bucket=args.bucket,
        input_uri=args.input,
        output_uri=args.output,
        cluster_id=args.cluster_id,
    )

    print(f"\n  ✓ Cluster ID : {cluster_id}")
    if step_id:
        print(f"  ✓ Step   ID  : {step_id}")

    print(f"\n  Ver en consola AWS:")
    print(f"  https://{args.region}.console.aws.amazon.com/emr/home?region={args.region}#/clusterDetails/{cluster_id}")

    if not args.no_wait:
        state = wait_for_cluster(emr_client, cluster_id)
        if state == "TERMINATED":
            print(f"\n  ✅ Job completado. Índice disponible en: {args.output}")
        else:
            print(f"\n  ⚠️  Cluster en estado {state}. Revisa los logs en s3://{args.bucket}/{LOG_PREFIX}/")
            sys.exit(1)


if __name__ == "__main__":
    main()
