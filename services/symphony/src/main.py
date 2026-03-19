import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
import uvicorn

from .config import settings
from .models import LinearWebhookPayload, LinearIssue, TaskState
from .linear import client as linear_client
from .linear.webhook import extract_issue_from_webhook, is_trigger_event, parse_webhook, validate_linear_webhook
from .agents.triage import triage
from .agents.code_agent import run_code_agent
from .agents.research_agent import run_research_agent
from .agents.deploy_agent import run_deploy_agent

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# In-memory task tracking
active_tasks: dict[str, TaskState] = {}
task_queue: asyncio.Queue[LinearIssue] = asyncio.Queue(maxsize=50)

WORKER_COUNT = 3


async def process_issue(issue: LinearIssue):
    """Full pipeline: triage -> agent(s) -> deploy agent."""
    task = TaskState(issue_id=issue.id, agent="triage", status="running")
    active_tasks[issue.id] = task

    try:
        # Step 1: Triage
        logger.info(f"Triaging issue {issue.id}: {issue.title}")
        triage_result = await triage(issue)
        task.agent = triage_result.agent

        # Step 2: Route to agent(s)
        agent_result = {"status": "pending", "result": ""}

        if triage_result.agent == "agent-gateway":
            logger.info(f"Routing {issue.id} to Agent Gateway")
            agent_result = await run_code_agent(issue, triage_result)

        elif triage_result.agent == "messaging-agent":
            logger.info(f"Routing {issue.id} to Messaging Agent")
            agent_result = await run_research_agent(issue, triage_result)

        elif triage_result.agent == "both":
            logger.info(f"Routing {issue.id} to Messaging Agent then Agent Gateway")
            research_result = await run_research_agent(issue, triage_result)
            # Feed research into code agent
            if research_result.get("status") == "done":
                issue.description = (
                    f"{issue.description or ''}\n\n"
                    f"## Research Findings\n{research_result.get('result', '')}"
                )
                agent_result = await run_code_agent(issue, triage_result)
            else:
                agent_result = research_result

        # Step 3: Deploy agent (approval gate)
        if agent_result.get("status") == "done":
            task.status = "waiting_approval"
            logger.info(f"Running deploy agent for {issue.id}")
            deploy_result = await run_deploy_agent(issue, agent_result, triage_result)
            task.status = "done" if deploy_result.get("status") == "done" else "failed"
            task.result = deploy_result.get("result")
        else:
            task.status = "failed"
            task.error = agent_result.get("error", "Agent returned non-done status")
            await linear_client.add_comment(
                issue.id,
                f"Symphony agent failed: {task.error}",
            )

    except Exception as e:
        logger.error(f"Pipeline error for {issue.id}: {e}")
        task.status = "failed"
        task.error = str(e)
        await linear_client.add_comment(issue.id, f"Symphony pipeline error: {e}")

    active_tasks[issue.id] = task


async def worker(worker_id: int):
    """Background worker that processes issues from the queue."""
    logger.info(f"Worker {worker_id} started")
    while True:
        issue = await task_queue.get()
        try:
            logger.info(f"Worker {worker_id} processing issue {issue.id}")
            await process_issue(issue)
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
        finally:
            task_queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start worker pool
    worker_tasks = [asyncio.create_task(worker(i)) for i in range(WORKER_COUNT)]
    logger.info(f"Symphony started on port {settings.symphony_port} with {WORKER_COUNT} workers")
    yield
    for t in worker_tasks:
        t.cancel()


app = FastAPI(title="Symphony", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "queue_depth": task_queue.qsize(),
        "active_tasks": len(active_tasks),
    }


@app.get("/status")
async def status():
    return {
        "queue_depth": task_queue.qsize(),
        "tasks": {
            tid: {
                "agent": t.agent,
                "status": t.status,
                "started_at": t.started_at.isoformat(),
                "error": t.error,
            }
            for tid, t in active_tasks.items()
        },
    }


@app.post("/webhook/linear")
async def linear_webhook(request: Request):
    # Use the shared webhook validation (handles signature check)
    body = await validate_linear_webhook(request)

    payload = parse_webhook(body)
    if not payload:
        raise HTTPException(status_code=400, detail="Invalid payload")

    issue = extract_issue_from_webhook(payload)
    if not is_trigger_event(payload, issue):
        return {"status": "ignored", "reason": "not a triggering status change"}

    # Repo allowlist: only process issues labeled with repos the user owns
    if settings.allowed_labels_set:
        issue_labels = {l.lower() for l in (issue.labels or [])}
        allowed = {l.lower() for l in settings.allowed_labels_set}
        if not issue_labels & allowed:
            logger.info(f"Skipping issue {issue.id}: labels {issue_labels} not in allowed repos")
            return {"status": "ignored", "reason": "issue not labeled with an allowed repo"}

    # Check if already processing
    if issue.id in active_tasks and active_tasks[issue.id].status == "running":
        return {"status": "already_processing", "issue_id": issue.id}

    # Queue the issue (non-blocking check for backpressure)
    if task_queue.full():
        logger.warning(f"Queue full ({task_queue.maxsize}), rejecting issue {issue.id}")
        raise HTTPException(status_code=503, detail="Queue full, try again later")

    await task_queue.put(issue)
    logger.info(f"Queued issue {issue.id}: {issue.title} (depth={task_queue.qsize()})")
    return {"status": "queued", "issue_id": issue.id}


def main():
    uvicorn.run(
        "src.main:app",
        host="127.0.0.1",
        port=settings.symphony_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
