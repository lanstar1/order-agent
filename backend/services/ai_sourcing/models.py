from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class TrendProfileInput(BaseModel):
    name: str = ""
    categoryCid: int
    categoryPath: str
    categoryDepth: int
    timeUnit: Literal["date", "week", "month"] = "month"
    devices: list[str] = Field(default_factory=list)
    genders: list[str] = Field(default_factory=list)
    ages: list[str] = Field(default_factory=list)
    spreadsheetId: str = ""
    resultCount: int | None = 20
    excludeBrandProducts: bool = False
    customExcludedTerms: list[str] = Field(default_factory=list)


class TrendProfile(BaseModel):
    id: str
    slug: str
    name: str
    status: str
    startPeriod: str
    endPeriod: str
    lastCollectedPeriod: str | None = None
    lastSyncedAt: str | None = None
    syncStatus: str = "idle"
    latestRunId: str | None = None
    resultCount: int
    excludeBrandProducts: bool
    customExcludedTerms: list[str]
    createdAt: str
    updatedAt: str
    categoryCid: int
    categoryPath: str
    categoryDepth: int
    timeUnit: str
    devices: list[str]
    genders: list[str]
    ages: list[str]
    spreadsheetId: str


class TrendCollectionTask(BaseModel):
    id: str
    runId: str
    profileId: str
    period: str
    status: str
    completedPages: int
    totalPages: int
    retryCount: int
    startedAt: str | None = None
    completedAt: str | None = None
    failureReason: str | None = None
    failureSnippet: str | None = None
    updatedAt: str


class TrendCollectionRun(BaseModel):
    id: str
    profileId: str
    status: str
    requestedBy: str
    runType: str
    startPeriod: str
    endPeriod: str
    totalTasks: int
    completedTasks: int
    failedTasks: int
    totalSnapshots: int
    sheetUrl: str | None = None
    startedAt: str | None = None
    completedAt: str | None = None
    cancelledAt: str | None = None
    failureReason: str | None = None
    createdAt: str
    updatedAt: str


class TrendKeywordSnapshot(BaseModel):
    id: str
    profileId: str
    runId: str
    taskId: str
    period: str
    rank: int
    keyword: str
    linkId: str
    categoryCid: int
    categoryPath: str
    devices: list[str]
    genders: list[str]
    ages: list[str]
    brandExcluded: bool = False
    collectedAt: str


class TrendCategoryNode(BaseModel):
    cid: int
    name: str
    fullPath: str
    level: int
    leaf: bool


class TrendRunDetail(TrendCollectionRun):
    profile: TrendProfile
    tasks: list[TrendCollectionTask] = Field(default_factory=list)
    snapshotsPreview: list[TrendKeywordSnapshot] = Field(default_factory=list)
    currentPeriod: str | None = None
    currentPage: int | None = None
    latestCompletedPeriod: str | None = None
    remainingTasks: int = 0
    averageTaskSeconds: int | None = None
    etaMinutes: int | None = None
    estimatedCompletionAt: str | None = None
    canCancel: bool = False
    canDelete: bool = False
    analysisReady: bool = False
    confidenceScore: int | None = None
    analysisSummary: dict[str, Any] | None = None
    analysisCards: list[dict[str, Any]] = Field(default_factory=list)


class TrendAdminMetric(BaseModel):
    id: str
    label: str
    value: str
    hint: str
    tone: str


class TrendAdminBoard(BaseModel):
    generatedAt: str
    metrics: list[TrendAdminMetric]
    profiles: list[TrendProfile]
    runs: list[TrendRunDetail]
