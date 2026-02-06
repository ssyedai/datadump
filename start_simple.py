
import subprocess
import os
import time
import sys
import socket

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def kill_port_process(port):
    if is_port_in_use(port):
        print(f"⚠️  Port {port} in use. Attempting to kill...")
        if os.name == 'nt':
            subprocess.run(f"for /f \"tokens=5\" %a in ('netstat -aon ^| find \":{port}\"') do taskkill /f /pid %a", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        else:
            subprocess.run(f"lsof -t -i:{port} | xargs kill -9", shell=True)
        time.sleep(1)

print("="*40)
print("🚀 Starting Bus Alert System (Python)")
print("="*40)

# 0. Proxy Port Forward
print("\n0️⃣ Bridging Kubernetes GPU Proxy...")
# Kill existing port-forward on Windows
if os.name == 'nt':
    subprocess.run("taskkill /f /im kubectl.exe /fi \"WINDOWTITLE eq PortForward*\"", shell=True, stderr=subprocess.DEVNULL)

# Start port-forward in background
with open("proxy_forward.log", "w", encoding="utf-8") as out, open("proxy_forward_error.log", "w", encoding="utf-8") as err:
    p0 = subprocess.Popen(["kubectl", "port-forward", "service/gpu-proxy", "30080:80"], stdout=out, stderr=err)
print(f"   ✅ GPU Proxy bridge started (PID: {p0.pid})")
time.sleep(3) # Wait for pf to establish

# 1. MinIO
print("\n1️⃣ Checking MinIO...")
if is_port_in_use(9000):
    print("   ✅ MinIO is running")
else:
    print("   🔸 Cleaning and starting MinIO Docker...")
    subprocess.run("docker rm -f minio", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    subprocess.run("docker run -d --name minio -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data --console-address :9001", shell=True)
    time.sleep(5)

# Setup Environment
base_dir = os.path.dirname(os.path.abspath(__file__))
venv_python = os.path.join(base_dir, "venv", "Scripts", "python.exe") if os.name == 'nt' else os.path.join(base_dir, "venv", "bin", "python")

if not os.path.exists(venv_python):
    print(f"⚠️  Venv python not found at {venv_python}. Falling back to sys.executable")
    venv_python = sys.executable

env = os.environ.copy()
env["MINIO_ENDPOINT"] = "localhost:9000"
env["MINIO_ACCESS_KEY"] = "minioadmin"
env["MINIO_SECRET_KEY"] = "minioadmin"
env["CONTAINER_API"] = "http://localhost:30080"
env["POLL_INTERVAL"] = "2"
env["PYTHONIOENCODING"] = "utf-8"

# 2. Upload API
print("\n2️⃣ Starting Upload API...")
kill_port_process(4000)
# Start in background
with open("upload_api.log", "w", encoding="utf-8") as out, open("upload_api_error.log", "w", encoding="utf-8") as err:
    p1 = subprocess.Popen([venv_python, "-m", "uvicorn", "upload_api:app", "--host", "0.0.0.0", "--port", "4000"], env=env, stdout=out, stderr=err)
print(f"   ✅ Upload API started (PID: {p1.pid})")

# Wait for Upload API to create bucket or do it manually here
time.sleep(2)

# 3. Consumer
print("\n3️⃣ Starting Consumer...")
# Kill existing consumer
if os.name == 'nt':
    subprocess.run("taskkill /f /im python.exe /fi \"WINDOWTITLE eq Consumer*\"", shell=True, stderr=subprocess.DEVNULL)
else:
    subprocess.run("pkill -f consumer.py", shell=True)

with open("consumer.log", "w", encoding="utf-8") as out, open("consumer_error.log", "w", encoding="utf-8") as err:
    p2 = subprocess.Popen([venv_python, "-u", "consumer.py"], env=env, stdout=out, stderr=err)
print(f"   ✅ Consumer running (PID: {p2.pid})")

# 4. Results API
print("\n4️⃣ Starting Results API...")
kill_port_process(8001)
with open("results_api.log", "w", encoding="utf-8") as out, open("results_api_error.log", "w", encoding="utf-8") as err:
    p3 = subprocess.Popen([venv_python, "-m", "uvicorn", "results_api:app", "--host", "0.0.0.0", "--port", "8001"], env=env, stdout=out, stderr=err)
print(f"   ✅ Results API started (PID: {p3.pid})")

print("\n" + "="*40)
print("✅ Services Started")
print("="*40)
print("\n🌐 URLs:")
print("   Upload:  http://localhost:4000")
print("   Results: http://localhost:8001")
print("   MinIO:   http://localhost:9001")
print("\n📋 Logs:")
print("   upload_api.log, consumer.log, results_api.log")
