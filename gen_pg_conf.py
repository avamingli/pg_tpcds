#!/usr/bin/env python3
"""Generate an optimized PostgreSQL configuration for TPC-DS/TPC-H benchmarks.

Auto-detects CPU, RAM, and disk type, then writes a conf file tuned for
analytical (OLAP/DW) workloads. The output file is named after the benchmark
(e.g. tpcds_postgres.conf) and can be included in postgresql.conf.

Usage:
    python3 gen_pg_conf.py              # writes tpcds_postgres.conf (auto-detected)
    python3 gen_pg_conf.py -o out.conf  # custom output path
    python3 gen_pg_conf.py --dry-run    # print to stdout only
"""

import argparse
import datetime
import os
import re
import shutil
import subprocess
import sys

# ---------------------------------------------------------------------------
# Benchmark auto-detection
# ---------------------------------------------------------------------------
BENCHMARKS = {
    "tpcds": "TPC-DS",
    "tpch":  "TPC-H",
}


def detect_benchmark(script_path):
    """Auto-detect benchmark from the script's filesystem location."""
    abspath = os.path.abspath(script_path).lower()
    for key in BENCHMARKS:
        if key in abspath:
            return key
    return None


# ---------------------------------------------------------------------------
# Hardware detection
# ---------------------------------------------------------------------------

def detect_cpu():
    """Return (vcpus, physical_cores)."""
    vcpus = os.cpu_count() or 1
    physical = vcpus

    try:
        out = subprocess.check_output(["lscpu"], text=True)
        cores_per_socket = sockets = 1
        for line in out.splitlines():
            if line.startswith("Core(s) per socket:"):
                cores_per_socket = int(line.split(":")[1])
            elif line.startswith("Socket(s):"):
                sockets = int(line.split(":")[1])
        physical = cores_per_socket * sockets
    except Exception:
        pass

    return vcpus, max(physical, 1)


def detect_ram_gb():
    """Return total RAM in GB (integer)."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return max(1, kb // (1024 * 1024))
    except Exception:
        pass
    try:
        out = subprocess.check_output(["free", "-g"], text=True)
        for line in out.splitlines():
            if line.startswith("Mem:"):
                return max(1, int(line.split()[1]))
    except Exception:
        pass
    return 4


def _mount_device(path):
    """Find the block device for the filesystem containing path."""
    try:
        out = subprocess.check_output(["df", path], text=True)
        dev = out.splitlines()[1].split()[0]
        return re.sub(r"\d+$", "", os.path.basename(dev))
    except Exception:
        return None


def detect_disk_type(path):
    """Return 'ssd' or 'hdd' for the device backing path."""
    dev = _mount_device(path)
    if dev:
        try:
            with open(f"/sys/block/{dev}/queue/rotational") as f:
                return "hdd" if f.read().strip() == "1" else "ssd"
        except Exception:
            pass
    return "hdd"


def detect_disk_free_gb(path):
    """Return free disk space in GB."""
    try:
        st = os.statvfs(path)
        return st.f_bavail * st.f_frsize // (1024 ** 3)
    except Exception:
        return 100


def find_pg_data_dir():
    """Try to discover the PostgreSQL data directory."""
    pgdata = os.environ.get("PGDATA")
    if pgdata and os.path.isdir(pgdata):
        return pgdata

    psql = shutil.which("psql")
    if psql:
        try:
            out = subprocess.check_output(
                [psql, "-t", "-A", "-c", "SHOW data_directory"],
                text=True, stderr=subprocess.DEVNULL, timeout=5,
            ).strip()
            if out and os.path.isdir(out):
                return out
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Configuration calculation
# ---------------------------------------------------------------------------

def fmt_size(gb):
    """Format a GB value to a PostgreSQL size string."""
    if gb >= 1:
        return f"{int(gb)}GB"
    return f"{max(1, int(gb * 1024))}MB"


def calc_config(vcpus, cores, ram_gb, disk_type, disk_free_gb):
    """Calculate optimal parameters. Returns list of (section, key, value, comment)."""
    params = []

    def add(section, key, value, comment=""):
        params.append((section, key, value, comment))

    # --- Memory ---
    add("Memory", "shared_buffers", fmt_size(ram_gb * 0.25), "25% of RAM")
    add("Memory", "effective_cache_size", fmt_size(ram_gb * 0.75), "75% of RAM")
    add("Memory", "work_mem", fmt_size(min(2, ram_gb / 32)),
        "RAM/32, each parallel worker gets this")
    add("Memory", "maintenance_work_mem", fmt_size(min(4, ram_gb / 16)),
        "for CREATE INDEX, VACUUM")

    # --- Parallelism ---
    add("Parallelism", "max_worker_processes", str(vcpus),
        f"{vcpus} vCPUs")
    add("Parallelism", "max_parallel_workers", str(cores),
        f"{cores} physical cores")
    add("Parallelism", "max_parallel_workers_per_gather",
        str(max(2, min(8, cores // 4))), "per-query parallelism")
    add("Parallelism", "max_parallel_maintenance_workers",
        str(max(2, min(4, cores // 8))), "parallel index builds")

    # --- Planner / IO ---
    if disk_type == "ssd":
        add("Planner", "random_page_cost", "1.1", "SSD")
        add("Planner", "effective_io_concurrency", "200", "SSD")
    else:
        add("Planner", "random_page_cost", "4.0", "HDD")
        add("Planner", "effective_io_concurrency", "2", "HDD")

    # --- WAL / Checkpoint ---
    add("WAL", "max_wal_size", fmt_size(min(40, max(2, disk_free_gb // 10))),
        "avoid frequent checkpoints during benchmark")
    add("WAL", "checkpoint_timeout", "'30min'", "")
    add("WAL", "checkpoint_completion_target", "0.9",
        "spread checkpoint writes")

    # --- Miscellaneous ---
    add("Misc", "huge_pages", "'try'",
        "'on' for best performance (requires OS huge pages setup)")
    add("Misc", "jit", "off",
        "JIT overhead > benefit for TPC-DS/TPC-H")

    return params


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def format_conf(params, bench_label, hw_summary, output_path, pg_data_dir):
    """Format parameters as a postgresql.conf include file."""
    lines = []
    lines.append(f"# {bench_label} — PostgreSQL Benchmark Configuration")
    lines.append(f"# Generated by gen_pg_conf.py on {datetime.date.today()}")
    lines.append(f"# Hardware: {hw_summary}")
    lines.append("#")
    abs_output = os.path.abspath(output_path)
    if pg_data_dir:
        lines.append("# To apply, add this line to postgresql.conf:")
        lines.append(f"#   include = '{abs_output}'")
        lines.append("# Then restart PostgreSQL:")
        lines.append(f"#   pg_ctl restart -D {pg_data_dir}")
    else:
        lines.append("# To apply, add this line to postgresql.conf:")
        lines.append(f"#   include = '{abs_output}'")
        lines.append("# Then restart PostgreSQL.")
    lines.append("")

    current_section = None
    for section, key, value, comment in params:
        if section != current_section:
            if current_section is not None:
                lines.append("")
            lines.append(f"# --- {section} ---")
            current_section = section
        entry = f"{key} = {value}"
        if comment:
            entry = f"{entry:<45} # {comment}"
        lines.append(entry)

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Generate optimized PostgreSQL config for TPC-DS/TPC-H benchmarks")
    ap.add_argument("-o", "--output", default=None,
                    help="Output file path (default: <benchmark>_postgres.conf)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print config to stdout without writing a file")
    args = ap.parse_args()

    # Detect benchmark
    bench = detect_benchmark(__file__)
    bench_label = BENCHMARKS.get(bench, "TPC Benchmark")

    # Default output filename
    if args.output is None:
        args.output = f"{bench}_postgres.conf" if bench else "postgres_bench.conf"

    # Detect hardware
    vcpus, cores = detect_cpu()
    ram_gb = detect_ram_gb()
    pg_data_dir = find_pg_data_dir()
    disk_path = pg_data_dir or "/"
    disk_type = detect_disk_type(disk_path)
    disk_free_gb = detect_disk_free_gb(disk_path)

    hw_summary = (f"{vcpus} vCPU ({cores} cores), {ram_gb} GB RAM, "
                  f"{disk_type.upper()}")

    print(f"Detected: {hw_summary}")
    if pg_data_dir:
        print(f"PG data:  {pg_data_dir}")
    print()

    # Calculate
    params = calc_config(vcpus, cores, ram_gb, disk_type, disk_free_gb)

    # Format
    conf_text = format_conf(params, bench_label, hw_summary,
                            args.output, pg_data_dir)

    if args.dry_run:
        print(conf_text)
    else:
        with open(args.output, "w") as f:
            f.write(conf_text)
        print(f"Wrote {args.output}")
        print()
        if pg_data_dir:
            pg_conf = os.path.join(pg_data_dir, "postgresql.conf")
            abs_out = os.path.abspath(args.output)
            print("To apply:")
            print(f"  echo \"include = '{abs_out}'\" >> {pg_conf}")
            print(f"  pg_ctl restart -D {pg_data_dir}")
        else:
            print("To apply, add to postgresql.conf:")
            print(f"  include = '{os.path.abspath(args.output)}'")


if __name__ == "__main__":
    main()
