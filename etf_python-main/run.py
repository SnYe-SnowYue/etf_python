import subprocess
import time

script_path = "app.py"

while True:
    try:
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"腳本發生錯誤: {e}, 重新啟動中...")
        time.sleep(5)  # 等待 5 秒後重新啟動
