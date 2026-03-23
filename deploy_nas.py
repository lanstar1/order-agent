#!/usr/bin/env python3
"""
order-agent NAS 배포 스크립트
- 로컬에서 Docker 이미지 빌드 (ARM64/AMD64 자동 감지)
- paramiko로 NAS에 파일 전송
- NAS에서 Docker 컨테이너 실행
"""
import os
import sys
import time
import tarfile
import tempfile
import subprocess
from pathlib import Path

try:
    import paramiko
except ImportError:
    print("paramiko 필요: pip install paramiko")
    sys.exit(1)

# ── NAS 접속 정보 ──
NAS_HOST = "10.0.0.152"
NAS_PORT = 2222
NAS_USER = "admin"
NAS_PASS = "wlsgmd6192"

# ── 배포 경로 ──
REMOTE_DIR = "/volume1/lanstar/order-agent"
DOCKER_BIN = "/var/packages/Docker/target/usr/bin/docker"
CONTAINER_NAME = "order-agent"
IMAGE_NAME = "order-agent:latest"
HOST_PORT = 8001  # lanchat-tool이 8088 사용 중, order-agent는 8001
CONTAINER_PORT = 8000

# ── 로컬 프로젝트 ──
PROJECT_DIR = Path(__file__).parent


def ssh_connect():
    """SSH 연결"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(NAS_HOST, port=NAS_PORT, username=NAS_USER, password=NAS_PASS)
    print(f"[SSH] {NAS_HOST} 연결 성공")
    return ssh


def ssh_exec(ssh, cmd, check=True):
    """SSH 명령 실행"""
    _, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    rc = stdout.channel.recv_exit_status()
    if check and rc != 0 and err:
        print(f"  [WARN] {err}")
    return out, err, rc


def upload_project(ssh):
    """프로젝트 파일을 tar로 묶어 NAS에 전송"""
    print("[UPLOAD] 프로젝트 파일 압축 중...")

    # 전송할 디렉토리/파일 목록
    include = ["backend", "frontend", "data", "Dockerfile", "docker-compose.yml"]

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with tarfile.open(tmp_path, "w:gz") as tar:
            for item in include:
                full = PROJECT_DIR / item
                if full.exists():
                    tar.add(str(full), arcname=item)

        size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        print(f"[UPLOAD] 압축 완료: {size_mb:.1f}MB")

        # NAS에 디렉토리 생성
        ssh_exec(ssh, f"mkdir -p {REMOTE_DIR}")

        # SSH exec + base64 청크 전송 (SFTP 비활성화 NAS 대응)
        remote_tar = f"{REMOTE_DIR}/project.tar.gz"
        print(f"[UPLOAD] NAS로 전송 중 (base64 방식)...")
        ssh_exec(ssh, f"rm -f {remote_tar}")

        # SSH channel stdin으로 직접 파일 전송
        print(f"[UPLOAD] NAS로 전송 중 (stdin pipe 방식)...")
        remote_tar = f"{REMOTE_DIR}/project.tar.gz"
        transport = ssh.get_transport()
        chan = transport.open_session()
        chan.exec_command(f"cat > {remote_tar}")

        CHUNK_SIZE = 32 * 1024  # 32KB
        sent = 0
        with open(tmp_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                chan.sendall(chunk)
                sent += len(chunk)
                print(f"  전송 중... {sent/(1024*1024):.1f}/{size_mb:.1f}MB", end="\r")
        chan.shutdown_write()
        chan.recv_exit_status()
        chan.close()
        print(f"\n[UPLOAD] 전송 완료")

        # NAS에서 압축 해제
        ssh_exec(ssh, f"cd {REMOTE_DIR} && tar xzf project.tar.gz && rm project.tar.gz")
        print("[UPLOAD] 압축 해제 완료")

    finally:
        os.unlink(tmp_path)


def upload_env(ssh):
    """로컬 .env 파일을 NAS에 전송 (base64 방식)"""
    env_file = PROJECT_DIR / ".env"
    if not env_file.exists():
        print("[ENV] .env 파일 없음 - NAS에 직접 생성 필요")
        return

    remote_env = f"{REMOTE_DIR}/.env"
    transport = ssh.get_transport()
    chan = transport.open_session()
    chan.exec_command(f"cat > {remote_env}")
    with open(env_file, "rb") as f:
        chan.sendall(f.read())
    chan.shutdown_write()
    chan.recv_exit_status()
    chan.close()
    print("[ENV] .env 파일 전송 완료")


def docker_build(ssh):
    """NAS에서 Docker 이미지 빌드"""
    print("[DOCKER] 이미지 빌드 중... (1~3분 소요)")
    out, err, rc = ssh_exec(
        ssh,
        f"cd {REMOTE_DIR} && {DOCKER_BIN} build -t {IMAGE_NAME} .",
        check=False,
    )
    if rc != 0:
        print(f"[DOCKER] 빌드 실패:\n{err}")
        sys.exit(1)
    print("[DOCKER] 이미지 빌드 완료")


def docker_deploy(ssh):
    """기존 컨테이너 중지 후 새 컨테이너 실행"""
    # 기존 컨테이너 중지 및 제거
    print("[DOCKER] 기존 컨테이너 정리...")
    ssh_exec(ssh, f"{DOCKER_BIN} stop {CONTAINER_NAME} 2>/dev/null", check=False)
    ssh_exec(ssh, f"{DOCKER_BIN} rm {CONTAINER_NAME} 2>/dev/null", check=False)

    # 데이터 디렉토리 보장
    ssh_exec(ssh, f"mkdir -p {REMOTE_DIR}/data/uploads {REMOTE_DIR}/data/feedback")

    # 새 컨테이너 실행
    run_cmd = (
        f"{DOCKER_BIN} run -d"
        f" --name {CONTAINER_NAME}"
        f" --restart unless-stopped"
        f" -p {HOST_PORT}:{CONTAINER_PORT}"
        f" --env-file {REMOTE_DIR}/.env"
        f" -e TZ=Asia/Seoul"
        f" -v {REMOTE_DIR}/data:/app/data"
        f" --memory=512m"
        f" {IMAGE_NAME}"
    )
    out, err, rc = ssh_exec(ssh, run_cmd, check=False)
    if rc != 0:
        print(f"[DOCKER] 실행 실패:\n{err}")
        sys.exit(1)

    print(f"[DOCKER] 컨테이너 실행 완료: {CONTAINER_NAME}")
    print(f"[DOCKER] 접속: http://{NAS_HOST}:{HOST_PORT}")


def docker_cleanup(ssh):
    """미사용 Docker 이미지 정리"""
    ssh_exec(ssh, f"{DOCKER_BIN} image prune -f", check=False)
    print("[DOCKER] 미사용 이미지 정리 완료")


def check_health(ssh):
    """헬스체크"""
    print("[HEALTH] 서버 시작 대기 (10초)...")
    time.sleep(10)
    out, err, rc = ssh_exec(
        ssh,
        f"curl -sf http://localhost:{HOST_PORT}/health || echo FAIL",
        check=False,
    )
    if "FAIL" in out or rc != 0:
        print("[HEALTH] 서버 아직 준비 안됨 - 로그 확인:")
        logs, _, _ = ssh_exec(ssh, f"{DOCKER_BIN} logs --tail 20 {CONTAINER_NAME}", check=False)
        print(logs)
    else:
        print(f"[HEALTH] 서버 정상 가동: http://{NAS_HOST}:{HOST_PORT}")


def main():
    print("=" * 50)
    print("  order-agent NAS 배포")
    print("=" * 50)

    ssh = ssh_connect()
    try:
        upload_project(ssh)
        upload_env(ssh)
        docker_build(ssh)
        docker_deploy(ssh)
        docker_cleanup(ssh)
        check_health(ssh)
    finally:
        ssh.close()

    print("\n" + "=" * 50)
    print(f"  배포 완료: http://{NAS_HOST}:{HOST_PORT}")
    print("=" * 50)


if __name__ == "__main__":
    main()
