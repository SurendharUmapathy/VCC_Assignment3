#!/usr/bin/env python3

import json
import os
import subprocess
import time
from datetime import datetime

import psutil


CHECK_INTERVAL = 30
THRESHOLD = 75.0
CONSECUTIVE_BREACHES_NEEDED = 4
COOLDOWN_SECONDS = 1800

STATE_FILE = "/home/demo/json03.json"
SCALE_SCRIPT = "/home/demo/autoscalevccassingmnt3.sh"

WATCH_CPU = True
WATCH_RAM = True
WATCH_DISK = False
WATCH_LOAD = False


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_state():
    fallback = {
        "breach_streak": 0,
        "last_scale_time": 0
    }

    if not os.path.exists(STATE_FILE):
        return fallback

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)

        if not isinstance(data, dict):
            return fallback

        return {
            "breach_streak": data.get("breach_streak", data.get("hit_count", 0)),
            "last_scale_time": data.get("last_scale_time", data.get("last_expand_at", 0)),
        }
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return fallback


def write_state(state):
    temp_file = STATE_FILE + ".tmp"

    payload = {
        "breach_streak": state["breach_streak"],
        "last_scale_time": state["last_scale_time"],
    }

    with open(temp_file, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj)

    os.replace(temp_file, STATE_FILE)


def fetch_metrics():
    cpu_pct = psutil.cpu_percent(interval=1)
    ram_pct = psutil.virtual_memory().percent
    disk_pct = psutil.disk_usage("/").percent

    normalized_load = None
    if hasattr(os, "getloadavg"):
        load_1m = os.getloadavg()[0]
        cpu_cores = psutil.cpu_count() or 1
        normalized_load = (load_1m / cpu_cores) * 100.0

    return {
        "cpu_pct": cpu_pct,
        "ram_pct": ram_pct,
        "disk_pct": disk_pct,
        "load_pct": normalized_load,
    }


def get_trigger_results(metrics):
    results = {}

    if WATCH_CPU:
        results["cpu"] = metrics["cpu_pct"] > THRESHOLD

    if WATCH_RAM:
        results["ram"] = metrics["ram_pct"] > THRESHOLD

    if WATCH_DISK:
        results["disk"] = metrics["disk_pct"] > THRESHOLD

    if WATCH_LOAD and metrics["load_pct"] is not None:
        results["load"] = metrics["load_pct"] > THRESHOLD

    return results


def threshold_crossed(metrics):
    checks = get_trigger_results(metrics)
    return any(checks.values()) if checks else False


def in_cooldown(last_scale_time):
    elapsed = time.time() - last_scale_time
    return elapsed < COOLDOWN_SECONDS


def launch_scale_process():
    print(f"[{current_timestamp()}] Scale action initiated.")

    try:
        result = subprocess.run(
            [SCALE_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except Exception as exc:
        print(f"[{current_timestamp()}] Failed to execute scale script: {exc}")
        return False

    if result.stdout:
        print(result.stdout)

    return result.returncode == 0


def print_status(metrics, exceeded, streak):
    load_display = metrics["load_pct"] if metrics["load_pct"] is not None else 0.0

    message = (
        f"[{current_timestamp()}] "
        f"CPU={metrics['cpu_pct']:.1f}% "
        f"RAM={metrics['ram_pct']:.1f}% "
        f"DISK={metrics['disk_pct']:.1f}% "
        f"LOAD={load_display:.1f}% "
        f"EXCEEDED={exceeded} "
        f"STREAK={streak}"
    )
    print(message)


def update_breach_streak(previous_streak, exceeded):
    return previous_streak + 1 if exceeded else 0


def maybe_scale(state):
    if state["breach_streak"] < CONSECUTIVE_BREACHES_NEEDED:
        return state

    if in_cooldown(state["last_scale_time"]):
        print(f"[{current_timestamp()}] Cooldown active. No scale action taken.")
        return state

    success = launch_scale_process()
    if success:
        state["last_scale_time"] = time.time()
        state["breach_streak"] = 0

    return state


def main():
    state = read_state()
    print("Autoscaling monitor is running.")

    while True:
        metrics = fetch_metrics()
        exceeded = threshold_crossed(metrics)

        state["breach_streak"] = update_breach_streak(
            state["breach_streak"],
            exceeded
        )

        print_status(metrics, exceeded, state["breach_streak"])

        state = maybe_scale(state)
        write_state(state)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()