#!/usr/bin/env python3
"""
GitHub Webhook 수신 → order-agent 자동 배포 서버
NAS에서 별도 컨테이너로 실행됨
"""
import hashlib
import hmac
import json
import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── 설정 ──
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
REPO_URL = os.getenv("REPO_URL", "https://github.com/lanstar1/order-agent.git")
REPO_BRANCH = os.getenv("REPO_BRANCH", "main")
PROJECT_DIR = "/app/repo"
DOCKER_BIN = "/usr/local/bin/docker"
CONTAINER_NAME = "order-agent"
IMAGE_NAME = "order-agent:latest"
HOST_PORT = "8001"
CONTAINER_PORT = "8000"
DATA_DIR = "/volume1/lanstar/order-agent/data"
ENV_FILE = "/volume1/lanstar/order-agent/.env"
LOG_FILE = "/app/logs/deploy.log"
PORT = int(os.getenv("PORT", "9000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("webhook")

# 배포 중복 방지
_deploying = False


def verify_signature(payload, signature):
    """GitHub HMAC-SHA256 서명 검증"""
    if not WEBHOOK_SECRET:
        log.warning("WEBHOOK_SECRET 미설정 - 서명 검증 건너뜀")
        return True
    if not signature:
        return False
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def deploy():
    """git clone → docker build → restart container"""
    global _deploying
    if _deploying:
        log.info("이미 배포 진행 중 - 건너뜀")
        return
    _deploying = True
    start = time.time()

    try:
        log.info("=" * 50)
        log.info("배포 시작")

        # 1. Git clone/pull
        if os.path.exists(f"{PROJECT_DIR}/.git"):
            log.info("[GIT] pull...")
            subprocess.run(
                ["git", "-C", PROJECT_DIR, "fetch", "--all"],
                check=True, capture_output=True, timeout=60,
            )
            subprocess.run(
                ["git", "-C", PROJECT_DIR, "reset", "--hard", f"origin/{REPO_BRANCH}"],
                check=True, capture_output=True, timeout=30,
            )
        else:
            log.info("[GIT] clone...")
            os.makedirs(PROJECT_DIR, exist_ok=True)
            subprocess.run(
                ["git", "clone", "--depth", "1", "-b", REPO_BRANCH, REPO_URL, PROJECT_DIR],
                check=True, capture_output=True, timeout=120,
            )

        # 최신 커밋 로그
        result = subprocess.run(
            ["git", "-C", PROJECT_DIR, "log", "--oneline", "-1"],
            capture_output=True, text=True,
        )
        log.info(f"[GIT] 최신 커밋: {result.stdout.strip()}")

        # 2. Docker build
        log.info("[DOCKER] 이미지 빌드 중...")
        subprocess.run(
            [DOCKER_BIN, "build", "-t", IMAGE_NAME, PROJECT_DIR],
            check=True, capture_output=True, timeout=600,
        )
        log.info("[DOCKER] 빌드 완료")

        # 3. 기존 컨테이너 중지 & 제거
        log.info("[DOCKER] 기존 컨테이너 교체...")
        subprocess.run(
            [DOCKER_BIN, "stop", CONTAINER_NAME],
            capture_output=True, timeout=30,
        )
        subprocess.run(
            [DOCKER_BIN, "rm", CONTAINER_NAME],
            capture_output=True, timeout=10,
        )

        # 4. 새 컨테이너 실행
        subprocess.run([
            DOCKER_BIN, "run", "-d",
            "--name", CONTAINER_NAME,
            "--restart", "unless-stopped",
            "-p", f"{HOST_PORT}:{CONTAINER_PORT}",
            "--env-file", ENV_FILE,
            "-e", "TZ=Asia/Seoul",
            "-v", f"{DATA_DIR}:/app/data",
            "--memory=512m",
            IMAGE_NAME,
        ], check=True, capture_output=True, timeout=30)
        log.info("[DOCKER] 새 컨테이너 시작")

        # 5. 헬스체크 (최대 30초 대기)
        for i in range(6):
            time.sleep(5)
            try:
                result = subprocess.run(
                    ["curl", "-sf", f"http://localhost:{HOST_PORT}/health"],
                    capture_output=True, text=True, timeout=5,
                )
                if result.returncode == 0:
                    log.info(f"[HEALTH] 서버 정상 ({(i+1)*5}초)")
                    break
            except Exception:
                pass
        else:
            log.warning("[HEALTH] 30초 내 서버 응답 없음")

        # 6. 미사용 이미지 정리
        subprocess.run(
            [DOCKER_BIN, "image", "prune", "-f"],
            capture_output=True, timeout=30,
        )

        elapsed = time.time() - start
        log.info(f"배포 완료 ({elapsed:.0f}초)")

    except subprocess.TimeoutExpired as e:
        log.error(f"배포 타임아웃: {e}")
    except subprocess.CalledProcessError as e:
        log.error(f"배포 실패: {e}")
        if e.stderr:
            log.error(f"  stderr: {e.stderr.decode()[:500]}")
    except Exception as e:
        log.error(f"배포 오류: {e}")
    finally:
        _deploying = False


class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """상태 확인"""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "ok",
                "deploying": _deploying,
                "timestamp": datetime.now().isoformat(),
            }).encode())
        elif self.path == "/logs":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            try:
                with open(LOG_FILE, "r") as f:
                    lines = f.readlines()
                self.wfile.write("".join(lines[-50:]).encode())
            except FileNotFoundError:
                self.wfile.write(b"No logs yet")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        """GitHub webhook 수신"""
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", 0))
        payload = self.rfile.read(content_length)

        # 서명 검증
        signature = self.headers.get("X-Hub-Signature-256", "")
        if not verify_signature(payload, signature):
            log.warning(f"서명 검증 실패: {self.client_address[0]}")
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Invalid signature")
            return

        # 이벤트 파싱
        event = self.headers.get("X-GitHub-Event", "")
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            return

        # push to main만 처리
        if event == "push":
            ref = data.get("ref", "")
            if ref == f"refs/heads/{REPO_BRANCH}":
                pusher = data.get("pusher", {}).get("name", "unknown")
                log.info(f"Push 이벤트 수신: {pusher} → {REPO_BRANCH}")

                # 비동기 배포 (webhook 응답 즉시 반환)
                thread = threading.Thread(target=deploy, daemon=True)
                thread.start()

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "deploying"}).encode())
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Ignored: not main branch")
        elif event == "ping":
            log.info("GitHub ping 수신")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"pong")
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(f"Ignored event: {event}".encode())

    def log_message(self, format, *args):
        pass  # 기본 로그 무시 (커스텀 로거 사용)


if __name__ == "__main__":
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    server = HTTPServer(("0.0.0.0", PORT), WebhookHandler)
    log.info(f"Webhook 서버 시작: 0.0.0.0:{PORT}")
    log.info(f"Repo: {REPO_URL} (branch: {REPO_BRANCH})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("서버 종료")
