"""
docker_stats.py

Collects CPU and memory info from Docker containers.
"""
import subprocess
import json

# container names -> database label
CONTAINER_MAP = {
    "PostgreSQL": ["pg_standalone"],
    "Citus":      ["citus_coord", "citus_worker1", "citus_worker2"],
    "ScyllaDB":   ["scylla1", "scylla2", "scylla3"],
    "MongoDB":    ["mongo1", "mongo2", "mongo3"],
}

SETUP_INFO = {
    "PostgreSQL": "A single server",
    "Citus":      "A cluster of 3 nodes",
    "ScyllaDB":   "A cluster of 3 nodes",
    "MongoDB":    "A replica set of 3 nodes",
}

def get_container_stats(container_name):
    """Get memory usage and CPU count for a container."""
    try:
        out = subprocess.check_output(
            ["docker", "inspect", container_name,
             "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}"],
            text=True
        ).strip()
        nano_cpus, mem_limit = out.split()
        nano_cpus = int(nano_cpus)
        mem_limit = int(mem_limit)
    except Exception:
        nano_cpus, mem_limit = 0, 0

    # get actual memory usage from docker stats
    try:
        out = subprocess.check_output(
            ["docker", "stats", container_name, "--no-stream",
             "--format", "{{.MemUsage}}"],
            text=True
        ).strip()
        mem_usage = out.split("/")[0].strip()
    except Exception:
        mem_usage = "N/A"

    # get number of CPUs available
    try:
        out = subprocess.check_output(
            ["docker", "inspect", container_name,
             "--format", "{{.HostConfig.CpusetCpus}}"],
            text=True
        ).strip()
        if out:
            cpus = out
        else:
            # no CPU limit, get host CPU count
            out2 = subprocess.check_output(
                ["docker", "info", "--format", "{{.NCPU}}"],
                text=True
            ).strip()
            cpus = out2
    except Exception:
        cpus = "N/A"

    return {"mem_usage": mem_usage, "cpus": cpus}

def get_db_stats(db_name):
    """Get aggregated stats for a database (all its containers)."""
    containers = CONTAINER_MAP.get(db_name, [])
    total_mem = 0.0
    cpus = "N/A"
    mem_str = ""

    for c in containers:
        stats = get_container_stats(c)
        cpus = stats["cpus"]
        mem_str_raw = stats["mem_usage"]
        # parse memory like "123.4MiB" or "1.2GiB"
        try:
            if "GiB" in mem_str_raw:
                total_mem += float(mem_str_raw.replace("GiB", ""))
            elif "MiB" in mem_str_raw:
                total_mem += float(mem_str_raw.replace("MiB", "")) / 1024
        except Exception:
            pass

    mem_str = f"{total_mem:.2f} GiB"
    setup = SETUP_INFO.get(db_name, "?")
    return {"setup": setup, "cpus": cpus, "mem_usage": mem_str}

def print_db_info(db_name):
    """Print resource info for a database."""
    info = get_db_stats(db_name)
    print(f"  Database Setup : {info['setup']}")
    print(f"  CPU cores      : {info['cpus']}")
    print(f"  Memory usage   : {info['mem_usage']}")
    return info
