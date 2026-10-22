"""
Environment verification script for EV Charging Station Analytics project.
Checks Python version, Java/JAVA_HOME, Docker, Docker Compose, and required project directories.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def check_python_version() -> dict:
    major, minor, micro = sys.version_info[:3]
    version_str = f"{major}.{minor}.{micro}"
    status = "OK"
    notes = []

    # PySpark 3.5.x officially supports Python 3.8 to 3.12
    if major != 3 or minor < 9:
        status = "FAIL"
        notes.append("Python 3.9+ is recommended for this project.")
    elif minor > 12:
        status = "WARNING"
        notes.append(
            f"Detected Python {version_str}. PySpark 3.5.x and compiled wheels (cassandra-driver) "
            "recommend Python 3.11 for optimal compatibility. Please use a Python 3.11 virtual environment."
        )
    else:
        notes.append(f"Python {version_str} is within the supported range (3.9-3.12).")

    return {
        "component": "Python Interpreter",
        "version": version_str,
        "status": status,
        "executable": sys.executable,
        "notes": " ".join(notes),
    }


def check_java() -> dict:
    java_cmd = shutil.which("java")
    java_home = os.environ.get("JAVA_HOME", "")

    if not java_cmd:
        return {
            "component": "Java Runtime",
            "version": "Not found",
            "status": "FAIL",
            "notes": "Java executable is not in PATH. PySpark requires Java 8, 11, or 17.",
        }

    try:
        proc = subprocess.run(
            [java_cmd, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5,
        )
        first_line = proc.stdout.splitlines()[0] if proc.stdout else "Unknown version"
        status = "OK"
        notes = [f"Output: {first_line}"]
        if java_home:
            notes.append(f"JAVA_HOME is set to: {java_home}")
        else:
            status = "WARNING"
            notes.append("JAVA_HOME is not set. It is strongly recommended to set JAVA_HOME for PySpark.")

        return {
            "component": "Java Runtime",
            "version": first_line,
            "status": status,
            "notes": " | ".join(notes),
        }
    except Exception as exc:
        return {
            "component": "Java Runtime",
            "version": "Error running java",
            "status": "FAIL",
            "notes": str(exc),
        }


def check_docker() -> dict:
    docker_cmd = shutil.which("docker")
    if not docker_cmd:
        return {
            "component": "Docker Engine",
            "version": "Not found",
            "status": "FAIL",
            "notes": "Docker CLI not found in PATH.",
        }

    try:
        proc = subprocess.run(
            [docker_cmd, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )
        version_str = proc.stdout.strip()

        # Check if Docker daemon is responsive
        info_proc = subprocess.run(
            [docker_cmd, "info"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )
        if info_proc.returncode == 0:
            status = "OK"
            notes = "Docker daemon is running and responsive."
        else:
            status = "WARNING"
            notes = "Docker CLI detected, but Docker daemon appears stopped or unreachable."

        return {
            "component": "Docker Engine",
            "version": version_str,
            "status": status,
            "notes": notes,
        }
    except Exception as exc:
        return {
            "component": "Docker Engine",
            "version": "Unknown",
            "status": "FAIL",
            "notes": str(exc),
        }


def check_docker_compose() -> dict:
    docker_cmd = shutil.which("docker")
    if not docker_cmd:
        return {
            "component": "Docker Compose",
            "version": "Not found",
            "status": "FAIL",
            "notes": "Docker not installed.",
        }

    try:
        proc = subprocess.run(
            [docker_cmd, "compose", "version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return {
                "component": "Docker Compose",
                "version": proc.stdout.strip(),
                "status": "OK",
                "notes": "Docker Compose v2 plugin is available.",
            }
        else:
            return {
                "component": "Docker Compose",
                "version": "Error",
                "status": "FAIL",
                "notes": proc.stderr.strip(),
            }
    except Exception as exc:
        return {
            "component": "Docker Compose",
            "version": "Unknown",
            "status": "FAIL",
            "notes": str(exc),
        }


def check_directories(project_root: Path) -> dict:
    required_dirs = [
        "data/raw",
        "data/processed",
        "src",
        "src/analytics",
        "src/database",
        "src/dashboard",
        "cassandra",
        "tests",
        "docs",
    ]
    missing = [d for d in required_dirs if not (project_root / d).is_dir()]
    if not missing:
        return {
            "component": "Project Scaffold",
            "version": f"{len(required_dirs)}/{len(required_dirs)} directories",
            "status": "OK",
            "notes": "All required directories exist.",
        }
    else:
        return {
            "component": "Project Scaffold",
            "version": f"Missing {len(missing)} directories",
            "status": "FAIL",
            "notes": f"Missing: {', '.join(missing)}",
        }


def check_python_packages() -> dict:
    packages = ["pyspark", "cassandra", "pandas", "streamlit", "plotly", "dotenv", "pytest"]
    installed = []
    missing = []

    for pkg in packages:
        try:
            __import__(pkg)
            installed.append(pkg)
        except ImportError:
            missing.append(pkg)

    if not missing:
        status = "OK"
        notes = "All required Python libraries are installed in this environment."
    else:
        status = "INFO"
        notes = (
            f"Installed ({len(installed)}): {', '.join(installed) or 'None'}. "
            f"Not yet installed in current interpreter ({len(missing)}): {', '.join(missing)}. "
            "Run 'pip install -r requirements.txt' inside your target virtual environment."
        )

    return {
        "component": "Python Libraries",
        "version": f"{len(installed)}/{len(packages)} installed",
        "status": status,
        "notes": notes,
    }


def main():
    project_root = Path(__file__).resolve().parent.parent
    print("=" * 80)
    print(" EV CHARGING STATION ANALYTICS - ENVIRONMENT CHECK ")
    print("=" * 80)
    print(f"Project Root: {project_root}\n")

    checks = [
        check_python_version(),
        check_java(),
        check_docker(),
        check_docker_compose(),
        check_directories(project_root),
        check_python_packages(),
    ]

    has_failures = False
    for c in checks:
        badge = f"[{c['status']}]"
        print(f"{badge:<10} {c['component']:<20}: {c['version']}")
        print(f"           Details: {c['notes']}")
        print("-" * 80)
        if c["status"] == "FAIL":
            has_failures = True

    print("\nSummary:")
    if has_failures:
        print("[!] Critical environment requirements are missing. Review the items marked [FAIL].")
        sys.exit(1)
    else:
        print("[+] Core environment scaffold checks passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
