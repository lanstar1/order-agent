"""Super Agent 열거형 정의"""
from enum import Enum

class JobType(str, Enum):
    SALES_ANALYSIS = "sales_analysis"
    CLIENT_ANALYSIS = "client_analysis"
    MARKET_RESEARCH = "market_research"
    MEETING_PREP = "meeting_prep"
    CONTENT_CREATION = "content_creation"
    INVENTORY_ANALYSIS = "inventory_analysis"
    PRICING_ANALYSIS = "pricing_analysis"
    CS_ANALYSIS = "cs_analysis"
    EXECUTIVE_REPORT = "executive_report"
    FREEFORM = "freeform"

class DeliverableType(str, Enum):
    REPORT = "report"
    SLIDES = "slides"
    SHEET = "sheet"
    BRIEF = "brief"
    EMAIL = "email"
    DASHBOARD = "dashboard"
    JSON = "json"

class JobStatus(str, Enum):
    RECEIVED = "received"
    PLANNING = "planning"
    EXECUTING = "executing"
    VERIFYING = "verifying"
    COMPOSING = "composing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"

class TaskKind(str, Enum):
    RESEARCH = "research"
    ANALYSIS = "analysis"
    RETRIEVAL = "retrieval"
    CALCULATION = "calculation"
    VERIFICATION = "verification"
    COMPOSITION = "composition"

class ArtifactType(str, Enum):
    PLAN = "plan"
    TASK_OUTPUT = "task_output"
    REPORT = "report"
    SLIDES = "slides"
    SHEET = "sheet"
    BRIEF = "brief"
    CHART = "chart"
    JSON_DATA = "json"
