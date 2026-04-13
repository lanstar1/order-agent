"""
Reels Generator — API 엔드포인트
content.py에 추가할 라우터 코드
"""
# ── content.py에 아래 엔드포인트 추가 ──

# from services.reels_generator import generate_reels, generate_threads_text

@router.post("/reels/generate-script")
async def generate_reels_script(data: dict, user: dict = Depends(get_current_user)):
    """에피소드 소재 → 릴스 스크립트 JSON 자동 생성"""
    from services.content_service import call_claude

    source_text = data.get("source_text", "")
    episode_num = data.get("episode_num", 1)

    prompt = f"""릴스 애니메이션 스크립트를 JSON으로 생성하세요.

설정:
- 주인공: 30대 후반 경영학과 출신 부사장, 코딩 제로 → Claude로 AI 개발
- Pixar 3D 애니메이션 스타일
- 35~45초, 7개 장면
- 각 장면: id, start, end, image(파일명), motion(zoom_in/zoom_out/pan_left_to_right/static), subtitle(자막), tts_text(나레이션), image_prompt(나노바나나용)
- 마지막 장면은 반전 또는 유머
- threads_text: 같은 이야기를 200~400자 텍스트로 (쓰레드 발행용)

소재: {source_text}
에피소드: EP.{episode_num:02d}

JSON만 출력 (```json 없이):"""

    result = await call_claude("릴스 스크립트 전문가. JSON만 출력.", prompt)

    try:
        script = json.loads(result.strip().strip("```json").strip("```"))
        conn = get_connection()
        try:
            conn.execute(
                "INSERT INTO content_items (platform, content_type, title, body, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'draft', datetime('now','localtime'), datetime('now','localtime'))",
                ("instagram", "reels", f"EP.{episode_num:02d}", json.dumps(script, ensure_ascii=False))
            )
            conn.commit()
        finally:
            conn.close()
        return {"script": script, "threads_text": script.get("threads_text", "")}
    except json.JSONDecodeError:
        return {"error": "JSON 파싱 실패", "raw": result[:500]}


@router.post("/reels/assemble")
async def assemble_reels(data: dict, user: dict = Depends(get_current_user)):
    """스크립트 JSON + 이미지 → ffmpeg로 릴스 MP4 생성"""
    from services.reels_generator import generate_reels

    item_id = data.get("item_id")
    bgm_name = data.get("bgm", "lofi_bright")

    conn = get_connection()
    try:
        row = conn.execute("SELECT body FROM content_items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            return {"error": "콘텐츠 없음"}
        script = json.loads(dict(row)["body"])
    finally:
        conn.close()

    images_dir = f"/home/claude/data/reels/ep{script.get('episode', 'XX').replace('EP.', '')}"
    output_path = f"/home/claude/data/reels/output/{script.get('episode', 'reels')}.mp4"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    bgm_path = f"/home/claude/data/reels/bgm/{bgm_name}.mp3"
    if not os.path.exists(bgm_path):
        bgm_path = None

    result = await generate_reels(script, images_dir, output_path, bgm_path)
    return result


@router.post("/reels/to-threads")
async def reels_to_threads(data: dict, user: dict = Depends(get_current_user)):
    """릴스 스크립트에서 쓰레드 텍스트 추출 + 콘텐츠 아이템 생성"""
    item_id = data.get("item_id")

    conn = get_connection()
    try:
        row = conn.execute("SELECT body, content_type FROM content_items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            return {"error": "콘텐츠 없음"}
        row = dict(row)
        if row["content_type"] != "reels":
            return {"error": "릴스 콘텐츠가 아님"}

        script = json.loads(row["body"])
        threads_text = script.get("threads_text", "")
        if not threads_text:
            parts = [s.get("tts_text", "") for s in script.get("scenes", []) if s.get("tts_text")]
            hashtags = " ".join(f"#{h}" for h in script.get("hashtags", []))
            threads_text = "\n\n".join(parts) + f"\n\n{hashtags}"

        conn.execute(
            "INSERT INTO content_items (source_id, platform, content_type, title, body, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'draft', datetime('now','localtime'), datetime('now','localtime'))",
            (item_id, "threads", "inertia_break", script.get("title", ""), threads_text)
        )
        conn.commit()
        return {"threads_text": threads_text, "message": "쓰레드 콘텐츠 생성됨"}
    finally:
        conn.close()
