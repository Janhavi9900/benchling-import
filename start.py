"""
start.py
Single command to start the entire Benchling Data Importer.
Usage: python start.py
"""
import subprocess
import sys
import os
import time
import webbrowser

def check_env():
    from dotenv import load_dotenv
    load_dotenv()
    missing = []
    if not os.getenv("BENCHLING_API_KEY"): missing.append("BENCHLING_API_KEY")
    if not os.getenv("ANTHROPIC_API_KEY"): missing.append("ANTHROPIC_API_KEY")
    if missing:
        print(f"\n⚠️  Missing environment variables: {missing}")
        print("   Add them to your .env file and try again.\n")
        sys.exit(1)
    print("✅ Environment variables loaded")

def check_erd():
    if not os.path.exists("ai/benchling_erd.json"):
        print("\n🧬 ERD not found — fetching from Benchling...")
        subprocess.run([sys.executable, "run_erd_fetch.py"], check=True)
    else:
        print("✅ ERD cache found")

def start_backend():
    print("🚀 Starting backend on http://localhost:8000")
    return subprocess.Popen([
        sys.executable, "-m", "uvicorn",
        "app.main:app",
        "--host", "0.0.0.0",
        "--port", "8000",
        "--reload"
    ])

def install_packages():
    print("📦 Installing Python packages...")
    subprocess.run([sys.executable, "-m", "pip", "install",
                    "fastapi", "uvicorn[standard]", "python-multipart",
                    "websockets", "-q"], check=True)
    print("✅ Packages ready")

if __name__ == "__main__":
    print("\n" + "="*50)
    print("  BENCHLING DATA IMPORTER")
    print("="*50)

    install_packages()
    check_env()
    check_erd()

    backend = start_backend()

    print("\n⏳ Waiting for server to start...")
    time.sleep(3)

    print("\n" + "="*50)
    print("  ✅ App is running!")
    print("  🌐 API:  http://localhost:8000")
    print("  📖 Docs: http://localhost:8000/docs")
    print("  Press Ctrl+C to stop")
    print("="*50 + "\n")

    webbrowser.open("http://localhost:8000/docs")

    try:
        backend.wait()
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        backend.terminate()