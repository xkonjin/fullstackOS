from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime, timezone


class LinearIssue(BaseModel):
    id: str
    title: str
    description: Optional[str] = None
    status: Optional[str] = None
    labels: list[str] = []
    url: Optional[str] = None
    repo_url: Optional[str] = None
    assignee: Optional[str] = None
    team_id: Optional[str] = None


class LinearWebhookPayload(BaseModel):
    action: str
    type: str
    data: dict
    url: Optional[str] = None
    createdAt: Optional[str] = None
    organizationId: Optional[str] = None
    webhookId: Optional[str] = None
    webhookTimestamp: Optional[int] = None


class TriageResult(BaseModel):
    agent: Literal["agent-gateway", "messaging-agent", "both"]
    priority: Literal["high", "medium", "low"]
    plan: str
    repo_url: Optional[str] = None
    research_question: Optional[str] = None


class TaskState(BaseModel):
    issue_id: str
    agent: str
    status: Literal["pending", "running", "waiting_approval", "done", "failed"]
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    result: Optional[str] = None
    pr_url: Optional[str] = None
    error: Optional[str] = None


class Agent GatewayTaskRequest(BaseModel):
    task: str
    repo_url: Optional[str] = None
    context: Optional[str] = None


class Agent GatewayTaskResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[str] = None
    pr_url: Optional[str] = None
    error: Optional[str] = None
