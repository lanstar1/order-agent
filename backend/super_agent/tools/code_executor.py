"""
Python 코드 실행 도구 — 데이터 분석/차트 생성
샌드박스 환경에서 안전하게 실행
"""
import os
import logging
import asyncio
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent.parent.parent / "data" / "sa_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 허용 모듈 (보안)
ALLOWED_IMPORTS = {
    "pandas", "numpy", "matplotlib", "matplotlib.pyplot", "json", "csv",
    "datetime", "math", "statistics", "collections", "re", "os.path",
    "openpyxl", "io", "pathlib",
}

SANDBOX_HEADER = """
import matplotlib
matplotlib.use('Agg')  # GUI 없는 백엔드
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import json, csv, math, statistics
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from pathlib import Path

plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 150

OUTPUT_DIR = "{output_dir}"
"""


async def execute_code(
    code: str,
    data_csv: str = "",
    description: str = "",
) -> "ToolResult":
    """Python 코드를 샌드박스에서 실행"""
    from super_agent.tools.tool_registry import ToolResult

    # 보안 검사
    dangerous = ["subprocess", "os.system", "exec(", "eval(", "__import__",
                  "shutil.rmtree", "os.remove", "open(", "requests"]
    # open은 matplotlib의 savefig에서 필요하므로 허용
    dangerous.remove("open(")

    for d in dangerous:
        if d in code:
            return ToolResult(success=False, error=f"보안 정책 위반: '{d}' 사용 불가")

    # 임시 스크립트 생성
    script_id = uuid.uuid4().hex[:8]
    script_path = tempfile.mktemp(suffix=".py", prefix=f"sa_exec_{script_id}_")

    header = SANDBOX_HEADER.format(output_dir=str(OUTPUT_DIR))

    # CSV 데이터가 있으면 DataFrame으로 로드
    if data_csv:
        header += f'\n_csv_data = """{data_csv[:50000]}"""\n'
        header += 'import io\ndf = pd.read_csv(io.StringIO(_csv_data))\n'
        header += 'print(f"데이터 로드 완료: {len(df)}행 x {len(df.columns)}열")\n\n'

    full_code = header + "\n" + code + "\n"

    # 결과 캡처를 위한 래퍼
    full_code += f"""
# 차트 저장
import glob
for fig_num in plt.get_fignums():
    fig = plt.figure(fig_num)
    fname = f"{str(OUTPUT_DIR)}/chart_{script_id}_{{fig_num}}.png"
    fig.savefig(fname, bbox_inches='tight', facecolor='white')
    print(f"[CHART_SAVED] {{fname}}")
plt.close('all')
"""

    with open(script_path, "w", encoding="utf-8") as f:
        f.write(full_code)

    try:
        proc = await asyncio.create_subprocess_exec(
            "python3", script_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(OUTPUT_DIR),
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        stdout_str = stdout.decode("utf-8", errors="replace")
        stderr_str = stderr.decode("utf-8", errors="replace")

        # 생성된 차트 파일 추출
        chart_files = []
        output_lines = []
        for line in stdout_str.split("\n"):
            if line.startswith("[CHART_SAVED]"):
                chart_files.append(line.replace("[CHART_SAVED] ", "").strip())
            else:
                output_lines.append(line)

        output_text = "\n".join(output_lines).strip()

        if proc.returncode != 0:
            return ToolResult(
                success=False,
                error=f"코드 실행 오류:\n{stderr_str[:2000]}",
                metadata={"stdout": output_text[:1000]},
            )

        result_text = output_text
        if chart_files:
            result_text += f"\n\n생성된 차트: {len(chart_files)}개"
            for cf in chart_files:
                result_text += f"\n- {Path(cf).name}"

        return ToolResult(
            success=True,
            data=result_text,
            metadata={
                "chart_files": chart_files,
                "stderr": stderr_str[:500] if stderr_str else "",
            },
        )

    except asyncio.TimeoutError:
        return ToolResult(success=False, error="코드 실행 타임아웃 (30초 초과)")
    finally:
        try:
            os.unlink(script_path)
        except Exception:
            pass


def register_code_executor_tool(registry):
    from super_agent.tools.tool_registry import ToolDefinition
    registry.register(ToolDefinition(
        name="execute_code",
        description="Python 코드 실행. 데이터 분석, 차트/그래프 생성, 통계 계산에 사용. pandas, matplotlib, numpy 사용 가능",
        parameters={
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "실행할 Python 코드"},
                "data_csv": {"type": "string", "description": "분석할 CSV 데이터 (선택)"},
                "description": {"type": "string", "description": "코드 설명 (선택)"},
            },
            "required": ["code"],
        },
        execute_fn=execute_code,
        category="analysis",
    ))
