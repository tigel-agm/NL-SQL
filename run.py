"""
run.py

Run both FastAPI backend and Streamlit frontend concurrently.
"""
import subprocess
import sys
from dotenv import load_dotenv


def start_process(cmd, name):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def stream_logs(proc, name):
    for line in proc.stdout:
        print(f"[{name}] {line}", end="")


def main():
    # Load environment variables from .env
    load_dotenv()

    processes = [
        (['uvicorn', 'main:app', '--reload', '--port', '8001'], 'backend'),
        (['streamlit', 'run', 'streamlit_app.py', '--server.port', '8502'], 'frontend'),
    ]
    procs = []
    try:
        # Start all processes
        for cmd, name in processes:
            print(f"Starting {name}...")
            proc = start_process(cmd, name)
            procs.append((proc, name))

        # Stream output until all exit or interrupted
        while True:
            alive = False
            for proc, name in procs:
                if proc.poll() is None:
                    alive = True
                    stream_logs(proc, name)
            if not alive:
                break
    except KeyboardInterrupt:
        print("\nInterrupted. Terminating processes...")
    finally:
        for proc, name in procs:
            if proc.poll() is None:
                proc.terminate()


if __name__ == '__main__':
    main()
