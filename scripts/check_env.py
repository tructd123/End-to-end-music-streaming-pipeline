#!/usr/bin/env python3
"""
Environment Detection Test Script
==================================
Run this script to verify environment configuration is correct.

Usage:
    python scripts/check_env.py
"""

import os
import sys
from pathlib import Path


def is_docker_environment() -> bool:
    """Check if running inside Docker container"""
    return (
        os.path.exists("/.dockerenv") or
        os.path.exists("/opt/dagster/credentials") or
        os.getenv("DAGSTER_HOME", "").startswith("/opt")
    )


def check_credentials():
    """Check GCP credentials configuration"""
    print("\n=== GCP Credentials ===")
    
    env_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print(f"GOOGLE_APPLICATION_CREDENTIALS env: {env_creds}")
    
    is_docker = is_docker_environment()
    
    # Local paths
    local_pipeline_key = Path(__file__).parent.parent / "credentials" / "pipeline-sa-key.json"
    local_dbt_key = Path(__file__).parent.parent / "credentials" / "dbt-sa-key.json"
    
    # Docker paths  
    docker_pipeline_key = Path("/opt/dagster/credentials/pipeline-sa-key.json")
    docker_dbt_key = Path("/opt/dagster/credentials/dbt-sa-key.json")
    
    if is_docker:
        print("\n  Docker Environment - checking Docker paths:")
        for name, path in [("pipeline-sa-key.json", docker_pipeline_key), 
                           ("dbt-sa-key.json", docker_dbt_key)]:
            status = "[OK]" if path.exists() else "[MISSING]"
            print(f"    {status} {path}")
    else:
        print("\n  Local Environment - checking local paths:")
        for name, path in [("pipeline-sa-key.json", local_pipeline_key), 
                           ("dbt-sa-key.json", local_dbt_key)]:
            status = "[OK]" if path.exists() else "[MISSING]"
            print(f"    {status} {path}")
        print("\n  (Docker paths will be available when running in container)")
    
    # Check env var points to valid file
    if env_creds:
        if Path(env_creds).exists():
            print(f"\n  [OK] Env var points to valid file")
        else:
            print(f"\n  [WARN] Env var file not found: {env_creds}")


def check_kafka():
    """Check Kafka/Redpanda connectivity settings"""
    print("\n=== Kafka/Redpanda ===")
    
    env_kafka = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    print(f"KAFKA_BOOTSTRAP_SERVERS env: {env_kafka}")
    
    if is_docker_environment():
        expected = "redpanda:29092"
        print(f"Docker detected - should use: {expected}")
    else:
        expected = "localhost:9092"
        print(f"Local detected - should use: {expected}")
    
    if env_kafka:
        if env_kafka == expected:
            print(f"  [OK] Configuration correct")
        else:
            print(f"  [WARN] Env var is '{env_kafka}', expected '{expected}'")
    else:
        print(f"  [INFO] No env var set, will default to '{expected}'")


def check_postgres():
    """Check PostgreSQL connectivity settings"""
    print("\n=== PostgreSQL (Local Mode) ===")
    
    env_host = os.getenv("POSTGRES_HOST")
    print(f"POSTGRES_HOST env: {env_host}")
    
    if is_docker_environment():
        expected = "postgres"
        print(f"Docker detected - should use: {expected}")
    else:
        expected = "localhost"
        print(f"Local detected - should use: {expected}")


def check_dagster():
    """Check Dagster configuration"""
    print("\n=== Dagster ===")
    
    dagster_home = os.getenv("DAGSTER_HOME")
    print(f"DAGSTER_HOME env: {dagster_home}")
    
    if dagster_home:
        dagster_yaml = Path(dagster_home) / "dagster.yaml"
        workspace_yaml = Path(dagster_home) / "workspace.yaml"
        
        print(f"  dagster.yaml exists: {dagster_yaml.exists()}")
        print(f"  workspace.yaml exists: {workspace_yaml.exists()}")
    else:
        print("  [WARN] DAGSTER_HOME not set")
        print("  For local dev, set: DAGSTER_HOME=./dagster/dagster_home")


def check_paths():
    """Check important paths"""
    print("\n=== Path Detection ===")
    
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent
    
    print(f"Script location: {script_path}")
    print(f"Project root: {project_root}")
    
    paths_to_check = {
        "credentials/": project_root / "credentials",
        "dagster/": project_root / "dagster",
        "dbt/": project_root / "dbt",
        "spark_streaming/": project_root / "spark_streaming",
    }
    
    for name, path in paths_to_check.items():
        status = "[OK]" if path.exists() else "[--]"
        print(f"  {status} {name}: {path}")


def main():
    print("=" * 60)
    print("SoundFlow Environment Check")
    print("=" * 60)
    
    env_type = "Docker" if is_docker_environment() else "Local Development"
    print(f"\nDetected Environment: {env_type}")
    
    check_paths()
    check_credentials()
    check_kafka()
    check_postgres()
    check_dagster()
    
    print("\n" + "=" * 60)
    print("Check complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
