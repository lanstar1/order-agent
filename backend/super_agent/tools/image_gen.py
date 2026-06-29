"""
이미지 생성 도구 — Google Gemini Imagen (주), OpenAI DALL-E 3 (fallback)
"""
import os
import logging
import asyncio
import base64
import uuid
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent.parent.parent / "data" / "sa_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


async def _generate_gemini(prompt: str, size: str = "1024x1024") -> Dict[str, Any]:
    """Google Gemini Imagen으로 이미지 생성"""
    api_key = os.getenv("GOOGLE_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY 미설정")

    import httpx
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={api_key}",
            json={
                "contents": [{"parts": [{"text": f"Generate an image: {prompt}"}]}],
                "generationConfig": {"responseModalities": ["TEXT"]},
            },
        )
        resp.raise_for_status()
        data = resp.json()

    # Gemini text-to-image은 모델에 따라 다름 — 텍스트 설명으로 fallback
    text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
    return {"type": "text_description", "content": text, "model": "gemini", "cost": 0.002}


async def _generate_dalle(prompt: str, size: str = "1024x1024") -> Dict[str, Any]:
    """OpenAI DALL-E 3로 이미지 생성"""
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise ValueError("OPENAI_API_KEY 미설정")

    import httpx
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.openai.com/v1/images/generations",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "dall-e-3",
                "prompt": prompt,
                "n": 1,
                "size": size,
                "response_format": "b64_json",
            },
        )
        resp.raise_for_status()
        data = resp.json()

    img_data = data["data"][0]
    b64 = img_data.get("b64_json", "")
    revised_prompt = img_data.get("revised_prompt", prompt)

    # 파일 저장
    filename = f"img_{uuid.uuid4().hex[:8]}.png"
    filepath = str(OUTPUT_DIR / filename)
    with open(filepath, "wb") as f:
        f.write(base64.b64decode(b64))

    cost = 0.04 if size == "1024x1024" else 0.08  # DALL-E 3 가격
    return {
        "type": "image",
        "file_path": filepath,
        "file_name": filename,
        "revised_prompt": revised_prompt,
        "model": "dall-e-3",
        "cost": cost,
    }


async def generate_image(prompt: str, size: str = "1024x1024") -> "ToolResult":
    """이미지 생성 (DALL-E → Gemini 텍스트 설명 fallback)"""
    from super_agent.tools.tool_registry import ToolResult

    # DALL-E 우선 (실제 이미지 생성)
    try:
        result = await _generate_dalle(prompt, size)
        return ToolResult(
            success=True,
            data=f"이미지 생성 완료: {result['file_name']}\n프롬프트: {result['revised_prompt']}",
            cost=result["cost"],
            metadata={
                "file_path": result["file_path"],
                "file_name": result["file_name"],
                "model": result["model"],
            },
        )
    except Exception as e:
        logger.warning(f"[ImageGen] DALL-E 실패: {e}, Gemini 텍스트 설명 시도")

    # Gemini fallback (텍스트 설명)
    try:
        result = await _generate_gemini(prompt, size)
        return ToolResult(
            success=True,
            data=f"[이미지 생성 불가 — 텍스트 설명]\n{result['content']}",
            cost=result["cost"],
            metadata={"model": result["model"], "type": "text_description"},
        )
    except Exception as e2:
        return ToolResult(success=False, error=f"이미지 생성 실패: DALL-E({e}), Gemini({e2})")


def register_image_gen_tool(registry):
    from super_agent.tools.tool_registry import ToolDefinition
    registry.register(ToolDefinition(
        name="image_gen",
        description="AI 이미지 생성. 제품 이미지, 인포그래픽, 홍보 이미지 등 생성",
        parameters={
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "이미지 생성 프롬프트 (영어 권장)"},
                "size": {"type": "string", "description": "이미지 크기 (기본 1024x1024)"},
            },
            "required": ["prompt"],
        },
        execute_fn=generate_image,
        category="generation",
    ))
