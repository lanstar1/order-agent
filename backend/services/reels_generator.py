"""
Reels Generator — ffmpeg 기반 릴스 자동 생성 엔진
order-agent/backend/services/reels_generator.py

사용법:
    from services.reels_generator import generate_reels
    result = await generate_reels(script_json, images_dir, output_path)

의존성:
    - ffmpeg (시스템 설치)
    - Pretendard 폰트 (/usr/share/fonts/Pretendard-Bold.ttf)
"""
import os
import json
import subprocess
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── 설정 ──

CANVAS_W = 1080
CANVAS_H = 1920
FPS = 30
FONT_PATH = os.getenv("REELS_FONT_PATH", "/usr/share/fonts/Pretendard-Bold.ttf")
FONT_SIZE = 52
SUBTITLE_Y = "h*0.72"
BGM_VOLUME_NORMAL = 0.25
BGM_VOLUME_DUCK = 0.15
TTS_PROVIDER = os.getenv("TTS_PROVIDER", "google")  # google | naver
I2V_PROVIDER = os.getenv("I2V_PROVIDER", "ken_burns")  # kling | minimax | flow_manual | ken_burns
KLING_API_KEY = os.getenv("KLING_API_KEY", "")
MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")


def _get_default_motion_prompt() -> str:
    """DB에서 기본 모션 프롬프트 로드"""
    try:
        from db.database import get_connection
        conn = get_connection()
        row = conn.execute("SELECT content FROM prompt_templates WHERE category = 'video' AND key = 'motion_default'").fetchone()
        conn.close()
        if row:
            return dict(row)["content"]
    except Exception:
        pass
    return "subtle breathing motion, slight head movement, natural idle animation"


# ── 메인 생성 함수 ──

async def generate_reels(
    script: dict,
    images_dir: str,
    output_path: str,
    bgm_path: Optional[str] = None,
) -> dict:
    """
    스크립트 JSON + 이미지 폴더 → 최종 릴스 MP4 생성

    Args:
        script: 에피소드 스크립트 JSON (scenes[], duration_sec 등)
        images_dir: 장면 이미지가 있는 디렉토리 (scene_01.png ~ scene_N.png)
        output_path: 최종 MP4 출력 경로
        bgm_path: BGM 파일 경로 (없으면 무음)

    Returns:
        {"success": True, "output": output_path, "duration": float}
    """
    work_dir = tempfile.mkdtemp(prefix="reels_")
    try:
        scenes = script.get("scenes", [])
        if not scenes:
            return {"success": False, "error": "장면 없음"}

        # Step 1: 장면별 TTS 생성
        tts_files = []
        for scene in scenes:
            tts_text = scene.get("tts_text", "")
            if tts_text:
                tts_file = os.path.join(work_dir, f"tts_{scene['id']:02d}.mp3")
                await generate_tts(tts_text, tts_file)
                tts_files.append({"id": scene["id"], "file": tts_file})

        # Step 2: TTS 길이 측정 → 장면 타이밍 자동 조정
        scene_timings = calculate_timings(scenes, tts_files)

        # Step 3: 장면별 비디오 클립 생성 (AI 영상변환 or Ken Burns)
        clip_files = []
        for i, scene in enumerate(scenes):
            image_file = os.path.join(images_dir, scene.get("image", f"scene_{scene['id']:02d}.png"))
            if not os.path.exists(image_file):
                logger.warning(f"이미지 없음: {image_file}, 스킵")
                continue

            clip_file = os.path.join(work_dir, f"clip_{scene['id']:02d}.mp4")
            timing = scene_timings[scene["id"]]
            duration = timing["end"] - timing["start"]

            # AI 이미지→영상 변환 시도 (약간의 움직임)
            i2v_success = False
            if I2V_PROVIDER != "ken_burns":
                i2v_clip = os.path.join(work_dir, f"i2v_{scene['id']:02d}.mp4")
                i2v_success = await image_to_video(
                    image_path=image_file,
                    output_path=i2v_clip,
                    duration=min(duration, 5),  # AI 영상은 최대 5초
                    motion_prompt=scene.get("motion_prompt", _get_default_motion_prompt()),
                    provider=I2V_PROVIDER,
                )
                if i2v_success:
                    # AI 영상을 정확한 duration으로 조정 + 자막 추가
                    adjust_clip_duration(i2v_clip, clip_file, duration, scene.get("subtitle", ""))
                    clip_files.append(clip_file)
                    continue

            # 폴백: Ken Burns 효과
            create_scene_clip(
                image_path=image_file,
                output_path=clip_file,
                duration=duration,
                motion=scene.get("motion", "zoom_in"),
                motion_speed=scene.get("motion_speed", 0.03),
                subtitle=scene.get("subtitle", ""),
            )
            clip_files.append(clip_file)

        if not clip_files:
            return {"success": False, "error": "생성된 클립 없음"}

        # Step 4: 클립 연결 (concat)
        concat_file = os.path.join(work_dir, "concat.txt")
        with open(concat_file, "w") as f:
            for clip in clip_files:
                f.write(f"file '{clip}'\n")

        video_only = os.path.join(work_dir, "video_only.mp4")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", concat_file,
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-r", str(FPS),
            video_only
        ], capture_output=True)

        # Step 5: TTS 오디오 합성 (장면별 타이밍에 맞춰 배치)
        tts_combined = os.path.join(work_dir, "tts_combined.mp3")
        combine_tts_audio(tts_files, scene_timings, tts_combined, script.get("duration_sec", 45))

        # Step 6: 최종 믹싱 (영상 + TTS + BGM)
        mix_final(video_only, tts_combined, bgm_path, output_path)

        # Step 7: 쓰레드 텍스트 자동 생성
        threads_text = generate_threads_text(script)

        total_duration = get_duration(output_path)
        return {
            "success": True,
            "output": output_path,
            "duration": total_duration,
            "threads_text": threads_text,
        }

    except Exception as e:
        logger.error(f"릴스 생성 실패: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# ── 이미지→영상 AI 변환 (약간의 움직임) ──

async def image_to_video(image_path: str, output_path: str, duration: float, motion_prompt: str, provider: str) -> bool:
    """정적 이미지 → AI 영상 변환 (캐릭터 약간의 움직임)"""
    try:
        if provider == "kling":
            return await _kling_i2v(image_path, output_path, duration, motion_prompt)
        elif provider == "minimax":
            return await _minimax_i2v(image_path, output_path, duration, motion_prompt)
        elif provider == "flow_manual":
            logger.info(f"[I2V] Flow 수동 모드: {image_path} → Google Flow에서 수동 변환 필요")
            return False
        else:
            return False
    except Exception as e:
        logger.warning(f"[I2V] {provider} 실패: {e}, Ken Burns 폴백 사용")
        return False


async def _kling_i2v(image_path: str, output_path: str, duration: float, motion_prompt: str) -> bool:
    """Kling API를 통한 이미지→영상 변환"""
    if not KLING_API_KEY:
        return False
    import httpx, base64, time

    with open(image_path, "rb") as f:
        img_b64 = base64.b64encode(f.read()).decode()

    async with httpx.AsyncClient(timeout=120) as client:
        # 1. 생성 요청
        resp = await client.post(
            "https://api.klingai.com/v1/videos/image2video",
            headers={"Authorization": f"Bearer {KLING_API_KEY}", "Content-Type": "application/json"},
            json={
                "model_name": "kling-v2",
                "image": img_b64,
                "prompt": motion_prompt,
                "duration": str(min(int(duration), 5)),
                "aspect_ratio": "9:16",
                "mode": "std",
            },
        )
        data = resp.json()
        task_id = data.get("data", {}).get("task_id")
        if not task_id:
            logger.warning(f"Kling 작업 생성 실패: {data}")
            return False

        # 2. 폴링 (최대 2분)
        for _ in range(24):
            await asyncio.sleep(5)
            status_resp = await client.get(
                f"https://api.klingai.com/v1/videos/image2video/{task_id}",
                headers={"Authorization": f"Bearer {KLING_API_KEY}"},
            )
            status_data = status_resp.json()
            task_status = status_data.get("data", {}).get("task_status", "")
            if task_status == "succeed":
                video_url = status_data["data"]["task_result"]["videos"][0]["url"]
                # 3. 다운로드
                video_resp = await client.get(video_url)
                with open(output_path, "wb") as f:
                    f.write(video_resp.content)
                return True
            elif task_status == "failed":
                logger.warning(f"Kling 생성 실패: {status_data}")
                return False

    return False


async def _minimax_i2v(image_path: str, output_path: str, duration: float, motion_prompt: str) -> bool:
    """MiniMAX (Hailuo) API를 통한 이미지→영상 변환"""
    if not MINIMAX_API_KEY:
        return False
    import httpx, time

    async with httpx.AsyncClient(timeout=120) as client:
        # 1. 이미지 업로드
        with open(image_path, "rb") as f:
            upload_resp = await client.post(
                "https://api.minimax.io/v1/files/upload",
                headers={"Authorization": f"Bearer {MINIMAX_API_KEY}"},
                files={"file": (os.path.basename(image_path), f, "image/png")},
                data={"purpose": "video_generation"},
            )
        upload_data = upload_resp.json()
        file_id = upload_data.get("file", {}).get("file_id")
        if not file_id:
            return False

        # 2. 영상 생성 요청
        gen_resp = await client.post(
            "https://api.minimax.io/v1/video_generation",
            headers={"Authorization": f"Bearer {MINIMAX_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "video-01-live",
                "first_frame_image": file_id,
                "prompt": motion_prompt,
            },
        )
        gen_data = gen_resp.json()
        task_id = gen_data.get("task_id")
        if not task_id:
            return False

        # 3. 폴링
        for _ in range(30):
            await asyncio.sleep(4)
            status_resp = await client.get(
                f"https://api.minimax.io/v1/query/video_generation?task_id={task_id}",
                headers={"Authorization": f"Bearer {MINIMAX_API_KEY}"},
            )
            sd = status_resp.json()
            if sd.get("status") == "Success":
                video_url = sd.get("file_id", "")
                # 파일 다운로드
                dl_resp = await client.get(
                    f"https://api.minimax.io/v1/files/retrieve?file_id={video_url}",
                    headers={"Authorization": f"Bearer {MINIMAX_API_KEY}"},
                )
                dl_data = dl_resp.json()
                actual_url = dl_data.get("file", {}).get("download_url", "")
                if actual_url:
                    vid_resp = await client.get(actual_url)
                    with open(output_path, "wb") as f:
                        f.write(vid_resp.content)
                    return True
            elif sd.get("status") == "Fail":
                return False

    return False


def adjust_clip_duration(input_path: str, output_path: str, target_duration: float, subtitle: str):
    """AI 영상 클립을 정확한 길이로 조정 + 자막 추가 + 9:16 크롭"""
    filters = [f"scale={CANVAS_W}:{CANVAS_H}:force_original_aspect_ratio=decrease,pad={CANVAS_W}:{CANVAS_H}:(ow-iw)/2:(oh-ih)/2"]

    # 자막 추가
    if subtitle:
        safe_sub = subtitle.replace("'", "\\'").replace(":", "\\:")
        filters.append(
            f"drawtext=text='{safe_sub}':fontfile={FONT_PATH}:fontsize={FONT_SIZE}"
            f":fontcolor=white:borderw=3:bordercolor=black"
            f":x=(w-text_w)/2:y={SUBTITLE_Y}:box=1:boxcolor=black@0.4:boxborderw=12"
        )

    input_dur = get_duration(input_path)
    if input_dur < target_duration:
        # AI 영상이 짧으면 마지막 프레임 정지로 연장
        filters.insert(0, f"tpad=stop_mode=clone:stop_duration={target_duration - input_dur}")

    cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-vf", ",".join(filters),
        "-t", str(target_duration),
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-r", str(FPS),
        "-an",  # AI 영상의 기존 오디오 제거 (TTS로 교체)
        output_path,
    ]
    subprocess.run(cmd, capture_output=True)


import asyncio  # 상단에 이미 있을 수 있지만 안전하게


# ── Ken Burns 장면 클립 생성 (폴백) ──

def create_scene_clip(image_path, output_path, duration, motion, motion_speed, subtitle):
    """단일 이미지 → Ken Burns 효과 + 자막이 적용된 비디오 클립"""
    frames = int(duration * FPS)

    # 모션 필터 생성
    if motion == "zoom_in":
        zp = f"zoompan=z='1+{motion_speed}*on/{frames}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={frames}:s={CANVAS_W}x{CANVAS_H}:fps={FPS}"
    elif motion == "zoom_out":
        max_z = 1 + motion_speed * frames / frames * 5
        zp = f"zoompan=z='{max_z}-{motion_speed}*on/{frames}*3':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={frames}:s={CANVAS_W}x{CANVAS_H}:fps={FPS}"
    elif motion == "pan_left_to_right":
        zp = f"zoompan=z='1.2':x='iw*0.15*on/{frames}':y='ih/2-(ih/zoom/2)':d={frames}:s={CANVAS_W}x{CANVAS_H}:fps={FPS}"
    elif motion == "pan_right_to_left":
        zp = f"zoompan=z='1.2':x='iw*0.15*(1-on/{frames})':y='ih/2-(ih/zoom/2)':d={frames}:s={CANVAS_W}x{CANVAS_H}:fps={FPS}"
    else:
        zp = f"zoompan=z='1':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={frames}:s={CANVAS_W}x{CANVAS_H}:fps={FPS}"

    # 자막 필터
    filters = [zp]
    if subtitle:
        safe_sub = subtitle.replace("'", "\\'").replace(":", "\\:")
        sub_filter = (
            f"drawtext=text='{safe_sub}'"
            f":fontfile={FONT_PATH}"
            f":fontsize={FONT_SIZE}"
            f":fontcolor=white"
            f":borderw=3:bordercolor=black"
            f":x=(w-text_w)/2"
            f":y={SUBTITLE_Y}"
            f":box=1:boxcolor=black@0.4:boxborderw=12"
        )
        filters.append(sub_filter)

    filter_complex = ",".join(filters)

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", image_path,
        "-vf", filter_complex,
        "-t", str(duration),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-r", str(FPS),
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"클립 생성 실패: {result.stderr[:500]}")


# ── TTS 생성 ──

async def generate_tts(text: str, output_path: str):
    """텍스트 → TTS MP3 생성"""
    if TTS_PROVIDER == "google":
        await _google_tts(text, output_path)
    elif TTS_PROVIDER == "naver":
        await _naver_tts(text, output_path)
    else:
        await _gtts_fallback(text, output_path)


async def _google_tts(text: str, output_path: str):
    """Google Cloud TTS (API 키 필요)"""
    import httpx
    api_key = os.getenv("GOOGLE_TTS_API_KEY", "")
    if not api_key:
        return await _gtts_fallback(text, output_path)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"https://texttospeech.googleapis.com/v1/text:synthesize?key={api_key}",
            json={
                "input": {"text": text},
                "voice": {"languageCode": "ko-KR", "name": "ko-KR-Neural2-C", "ssmlGender": "MALE"},
                "audioConfig": {"audioEncoding": "MP3", "speakingRate": 1.05, "pitch": -1.0},
            },
        )
        data = resp.json()
        if "audioContent" in data:
            import base64
            audio_bytes = base64.b64decode(data["audioContent"])
            with open(output_path, "wb") as f:
                f.write(audio_bytes)
        else:
            await _gtts_fallback(text, output_path)


async def _naver_tts(text: str, output_path: str):
    """Naver Clova TTS"""
    import httpx
    client_id = os.getenv("NAVER_TTS_CLIENT_ID", "")
    client_secret = os.getenv("NAVER_TTS_CLIENT_SECRET", "")
    if not client_id:
        return await _gtts_fallback(text, output_path)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://naveropenapi.apigw.ntruss.com/tts-premium/v1/tts",
            headers={"X-NCP-APIGW-API-KEY-ID": client_id, "X-NCP-APIGW-API-KEY": client_secret},
            data={"speaker": "nkyunglee", "text": text, "volume": "0", "speed": "0", "pitch": "0", "format": "mp3"},
        )
        if resp.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(resp.content)
        else:
            await _gtts_fallback(text, output_path)


async def _gtts_fallback(text: str, output_path: str):
    """무료 gTTS 폴백"""
    try:
        from gtts import gTTS
        tts = gTTS(text=text, lang="ko", slow=False)
        tts.save(output_path)
    except Exception as e:
        logger.warning(f"gTTS 실패, 무음 생성: {e}")
        subprocess.run(["ffmpeg", "-y", "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono", "-t", "3", output_path], capture_output=True)


# ── 타이밍 계산 ──

def calculate_timings(scenes, tts_files):
    """TTS 오디오 길이 기반으로 장면 타이밍 자동 조정"""
    tts_durations = {}
    for tf in tts_files:
        dur = get_duration(tf["file"])
        tts_durations[tf["id"]] = dur

    timings = {}
    current_time = 0.0
    for scene in scenes:
        sid = scene["id"]
        specified_dur = scene.get("end", 0) - scene.get("start", 0)
        tts_dur = tts_durations.get(sid, 0)
        actual_dur = max(specified_dur, tts_dur + 0.5, 3.0)

        timings[sid] = {"start": current_time, "end": current_time + actual_dur}
        current_time += actual_dur

    return timings


def get_duration(file_path):
    """ffprobe로 파일 길이 측정"""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", file_path],
            capture_output=True, text=True,
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


# ── TTS 오디오 합성 ──

def combine_tts_audio(tts_files, timings, output_path, total_duration):
    """장면별 TTS를 타이밍에 맞춰 하나의 오디오 트랙으로 합성"""
    if not tts_files:
        subprocess.run(["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=mono", "-t", str(total_duration), output_path], capture_output=True)
        return

    filter_parts = []
    inputs = ["-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo:d={total_duration}"]
    input_idx = 1

    for tf in tts_files:
        inputs.extend(["-i", tf["file"]])
        timing = timings.get(tf["id"], {})
        delay_ms = int(timing.get("start", 0) * 1000)
        filter_parts.append(f"[{input_idx}]adelay={delay_ms}|{delay_ms}[d{input_idx}]")
        input_idx += 1

    mix_inputs = "[0]" + "".join(f"[d{i}]" for i in range(1, input_idx))
    filter_parts.append(f"{mix_inputs}amix=inputs={input_idx}:duration=first:dropout_transition=0[out]")

    filter_complex = ";".join(filter_parts)
    cmd = ["ffmpeg", "-y"] + inputs + ["-filter_complex", filter_complex, "-map", "[out]", output_path]
    subprocess.run(cmd, capture_output=True)


# ── 최종 믹싱 ──

def mix_final(video_path, tts_path, bgm_path, output_path):
    """비디오 + TTS + BGM 최종 믹싱"""
    if bgm_path and os.path.exists(bgm_path):
        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", tts_path,
            "-i", bgm_path,
            "-filter_complex",
            f"[1:a]volume=1.0[tts];"
            f"[2:a]volume={BGM_VOLUME_DUCK}[bgm];"
            f"[tts][bgm]amix=inputs=2:duration=first:dropout_transition=2[aout]",
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            "-shortest",
            output_path,
        ]
    else:
        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", tts_path,
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            "-shortest",
            output_path,
        ]

    subprocess.run(cmd, capture_output=True)


# ── 쓰레드 텍스트 자동 생성 ──

def generate_threads_text(script: dict) -> str:
    """릴스 스크립트에서 쓰레드용 텍스트 추출"""
    if script.get("threads_text"):
        return script["threads_text"]

    parts = []
    for scene in script.get("scenes", []):
        tts = scene.get("tts_text", "")
        if tts:
            parts.append(tts)

    text = "\n\n".join(parts)
    hashtags = " ".join(f"#{h}" for h in script.get("hashtags", []))
    return f"{text}\n\n{hashtags}"


# ── CLI 테스트 ──

if __name__ == "__main__":
    import asyncio

    test_script = {
        "episode": "EP.01",
        "title": "코딩을 1도 모르는 부사장이 개발자가 된 사연",
        "duration_sec": 42,
        "scenes": [
            {"id": 1, "start": 0, "end": 5, "image": "scene_01.png", "motion": "zoom_in", "subtitle": "나는 경영학과 출신이다", "tts_text": "나는 경영학과 출신이다. 코딩이 뭔지 1도 몰랐다."},
            {"id": 2, "start": 5, "end": 12, "image": "scene_02.png", "motion": "pan_left_to_right", "subtitle": "발주서는 원래 사람이 하는 일이었다", "tts_text": "발주서가 오면 PDF 열고 확인하고 ERP에 치고 송장 등록하고. 매일 2시간."},
            {"id": 3, "start": 12, "end": 18, "image": "scene_03.png", "motion": "zoom_in", "subtitle": "어느 날, Claude를 만났다", "tts_text": "어느 날 밤 AI한테 물어봤다. 이거 자동으로 할 수 있어?"},
            {"id": 4, "start": 18, "end": 23, "image": "scene_04.png", "motion": "static", "subtitle": "네, 만들어드릴까요?", "tts_text": "AI가 말했다. 만들어드릴까요?"},
            {"id": 5, "start": 23, "end": 30, "image": "scene_05.png", "motion": "zoom_in", "subtitle": "물론 쉽지는 않았다", "tts_text": "물론 쉽지 않았다. 기능 1개 만들면 버그 2.2개가 따라왔다."},
            {"id": 6, "start": 30, "end": 37, "image": "scene_06.png", "motion": "zoom_out", "subtitle": "2시간이 10분이 됐다", "tts_text": "발주서 2시간이 10분이 됐다. 경영학과 출신이 만든 시스템으로."},
            {"id": 7, "start": 37, "end": 42, "image": "scene_07.png", "motion": "zoom_in", "subtitle": "제가 하는 게 더 빠를 텐데요", "tts_text": "물류 담당자가 말했다. 제가 하는 게 더 빠를 텐데요."},
        ],
        "hashtags": ["관성깨기", "부사장코딩", "비개발자코딩", "AI자동화"]
    }

    print("=== Reels Generator CLI Test ===")
    print(f"Episode: {test_script['episode']}")
    print(f"Scenes: {len(test_script['scenes'])}")
    print(f"TTS lines: {sum(1 for s in test_script['scenes'] if s.get('tts_text'))}")
    print(f"\nThreads text preview:")
    print(generate_threads_text(test_script)[:200])
    print("\nTo generate: provide images in scenes/ directory and run:")
    print("  python reels_generator.py --script ep01.json --images ./scenes/ --output reels_ep01.mp4")
