"""Super Agent Pydantic 스키마"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# ─── Job 관련 ───
class CreateJobRequest(BaseModel):
    user_prompt: str = Field(..., description="사용자 자연어 요청")
    job_type: Optional[str] = Field(None, description="업무 유형 (자동 분류됨)")
    deliverable_type: Optional[str] = Field("report", description="결과물 유형")
    file_ids: Optional[List[str]] = Field(default_factory=list, description="업로드 파일 ID")
    constraints: Optional[Dict[str, Any]] = Field(default_factory=dict)

class JobResponse(BaseModel):
    job_id: str
    status: str
    job_type: Optional[str] = None
    deliverable_type: Optional[str] = None
    title: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None
    current_summary: Optional[str] = None
    created_at: Optional[str] = None

class JobListResponse(BaseModel):
    items: List[JobResponse]
    total: int

# ─── Task 관련 ───
class SubTask(BaseModel):
    task_id: str
    task_key: str
    task_kind: str
    title: str
    objective: str
    preferred_llm: str = "claude-sonnet"
    fallback_llm: str = "gpt-4o"
    required_tools: List[str] = Field(default_factory=list)
    depends_on: List[str] = Field(default_factory=list)
    timeout_sec: int = 60
    status: str = "pending"

class ExecutionPlan(BaseModel):
    job_id: str
    intent: str
    complexity: int = 3
    subtasks: List[SubTask]
    deliverable_type: str = "report"
    estimated_cost: float = 0.0

# ─── Artifact 관련 ───
class ArtifactResponse(BaseModel):
    artifact_id: str
    artifact_type: str
    title: Optional[str] = None
    content_format: str = "markdown"
    is_final: bool = False
    file_path: Optional[str] = None
    download_url: Optional[str] = None
    created_at: Optional[str] = None

# ─── WebSocket 메시지 ───
class WSProgressMessage(BaseModel):
    type: str = "progress"  # progress, task_update, completed, error
    job_id: str
    status: str
    message: str
    progress_pct: int = 0
    task_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
