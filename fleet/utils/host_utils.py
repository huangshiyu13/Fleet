import subprocess


def get_hostname():
    # 运行 hostname 命令
    result = subprocess.run(["hostname"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip()  # 去除多余的换行符
    else:
        return "Error: " + result.stderr


def get_ip_address():
    # 运行 hostname -i 命令
    result = subprocess.run(["hostname", "-i"], capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout.strip()  # 去除多余的换行符
    else:
        return "Error: " + result.stderr
