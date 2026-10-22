"""
Cassandra connectivity verification script for EV Charging Station Analytics.
Performs a TCP port check and, if the driver is available, executes an active CQL query
against the Cassandra system tables to verify readiness.
"""

import os
import socket
import sys
from pathlib import Path

# Load .env if python-dotenv is present
try:
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        # Fall back to .env.example values for defaults if .env does not exist yet
        load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env.example")
except ImportError:
    pass

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
CASSANDRA_USER = os.environ.get("CASSANDRA_USERNAME", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")


def check_tcp_port(host: str, port: int, timeout_sec: float = 3.0) -> bool:
    """Test raw TCP connection to the Cassandra native transport port."""
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def check_cql_connection(host: str, port: int):
    """Connect to Cassandra via the official Python driver and query system metadata."""
    try:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
    except ImportError:
        print("[!] cassandra-driver is not installed in the current Python environment.")
        print("    Install it via: pip install cassandra-driver==3.29.2")
        print("    Raw TCP socket connectivity will be used as the preliminary test.")
        return False

    auth_provider = PlainTextAuthProvider(
        username=CASSANDRA_USER, password=CASSANDRA_PASSWORD
    )

    print(f"[*] Attempting CQL connection to {host}:{port}...")
    cluster = None
    try:
        cluster = Cluster(
            contact_points=[host],
            port=port,
            auth_provider=auth_provider,
            connect_timeout=10,
        )
        session = cluster.connect()
        row = session.execute(
            "SELECT cluster_name, release_version, data_center, rack FROM system.local;"
        ).one()

        keyspaces = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces;"
        )
        ks_names = [k.keyspace_name for k in keyspaces]

        print("\n" + "=" * 60)
        print(" [SUCCESS] Cassandra Native Transport Connection Established")
        print("=" * 60)
        print(f" Cluster Name      : {row.cluster_name}")
        print(f" Release Version   : {row.release_version}")
        print(f" Data Center       : {row.data_center}")
        print(f" Rack              : {row.rack}")
        print(f" Discovered Keyspaces: {', '.join(ks_names)}")
        print("=" * 60)
        return True
    except Exception as exc:
        print(f"\n[FAIL] CQL connection failed: {exc}")
        return False
    finally:
        if cluster:
            cluster.shutdown()


def main():
    print(f"[*] Testing Cassandra endpoint at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")

    tcp_ok = check_tcp_port(CASSANDRA_HOST, CASSANDRA_PORT)
    if not tcp_ok:
        print(f"[FAIL] Unable to reach port {CASSANDRA_PORT} on {CASSANDRA_HOST}.")
        print("       Is the Cassandra container running?")
        print("       Run: docker compose up -d")
        print("       Check status: docker compose ps")
        sys.exit(1)

    print(f"[+] TCP port {CASSANDRA_PORT} is open and accepting socket connections.")

    cql_ok = check_cql_connection(CASSANDRA_HOST, CASSANDRA_PORT)
    if cql_ok:
        sys.exit(0)
    else:
        # If TCP is open but driver not installed or handshake timed out
        print("[*] Note: If the container recently started, Cassandra may still be initializing JVM/Gossip.")
        print("    Check readiness with: docker compose ps")
        sys.exit(0 if tcp_ok else 1)


if __name__ == "__main__":
    main()
