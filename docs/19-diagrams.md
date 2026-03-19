# System Diagrams

Reference diagrams for the full stack. Each shows a different cross-section of the system.

---

## 1. Full System Architecture

The complete component map from CLI client through to providers and autonomous subsystems.

```mermaid
graph TB
    subgraph "Your Machine"
        CC[Claude Code] --> CM[claudemax]
        CX[Codex CLI] --> CM
        DR[Droid] --> CM
        CM --> ORCH[Orchestrator :8318]
    end

    subgraph "Orchestrator"
        ORCH --> TR[Tier Router]
        TR --> LR[Learning Router]
        LR --> BG[Budget Guard]
        BG --> CB[Circuit Breaker]
        CB --> SM[Semaphore]
    end

    subgraph "Providers"
        SM --> CL[Claude API]
        SM --> CD[Codex API]
        SM --> GM[Gemini API]
        SM --> GL[GLM API]
        SM --> MM[MiniMax API]
        SM --> OR[OpenRouter]
    end

    subgraph "Auth"
        ORCH --> CP[CLIProxyAPI :8317]
        CP --> TK[Token Files]
    end

    subgraph "Fleet"
        ORCH --> FG[Fleet Gateway :4105]
        FG --> PE[Pipeline Engine]
        PE --> RL[RALPH Loop]
    end

    subgraph "Autonomous"
        SY[Symphony Poller] --> ORCH
        SY --> LN[Linear API]
        NC[Nanoclaw Daemon] --> CP
        NC --> ORCH
    end
```

All CLI clients (`claudemax`, Codex, Droid) point to the orchestrator as their `ANTHROPIC_BASE_URL`. The orchestrator handles routing, failover, and translation. CLIProxyAPI is the auth layer - it holds token files and handles OAuth refresh. Fleet Gateway is the dispatch target for pipeline work. Symphony and Nanoclaw are autonomous daemons that run independently and interact with the stack via its HTTP APIs.

---

## 2. Request Flow (Sequence)

A single request from Claude Code to provider and back, including the routing and failover path.

```mermaid
sequenceDiagram
    participant CC as Claude Code
    participant CM as claudemax
    participant O as Orchestrator
    participant TR as Tier Router
    participant P as Provider
    participant DB as Usage DB

    CC->>CM: POST /v1/messages
    CM->>O: Forward (ANTHROPIC_BASE_URL)
    O->>TR: classifyTier(model)
    TR->>TR: analyzeComplexity(content)
    TR->>TR: getNextRoute(tier, budget)
    O->>P: sendRequest(translated)
    alt Success
        P-->>O: Response
        O->>DB: Record usage
        O-->>CC: Response (model name rewritten)
    else Failure
        O->>TR: Exclude provider
        O->>P: Retry with next provider
        P-->>O: Response
        O-->>CC: Response
    end
```

The model name in the response is always rewritten to the name the caller sent. From the caller's perspective, the provider is invisible. Tier classification happens before routing - the router uses both the requested model name and the message content complexity to select the best available route.

---

## 3. RALPH Self-Correction Loop

The pipeline's built-in test-fix-retest cycle. Runs up to 5 times before escalating.

```mermaid
graph TD
    A[implement] --> B[test]
    B -->|pass| C[review]
    B -->|fail| D{cycle < 5?}
    D -->|yes| E[fix]
    E --> B
    D -->|no| F[escalate]
    C -->|pass| G[merge]
    C -->|fail| H[re-implement]
    H --> B
    G --> I[deploy]
```

`cycle_count` is the guard. It increments on every pass through the fix→test arc. At 5, the pipeline stops self-correcting and escalates to a human (or Symphony moves the ticket to Backlog). Review failures send the task back to implement, not just fix - they indicate a design problem, not a code defect.

---

## 4. Token Lifecycle

The full state machine for an OAuth token from initial login through expiry, refresh, and manual reauth.

```mermaid
stateDiagram-v2
    [*] --> Active: OAuth login
    Active --> NearExpiry: < 72h remaining
    NearExpiry --> Refreshed: CLIProxyAPI auto-refresh
    Refreshed --> Active
    NearExpiry --> Expired: Refresh failed
    Expired --> ManualReauth: Nanoclaw alert
    ManualReauth --> Active: Browser OAuth
    Active --> RateLimited: 429 from provider
    RateLimited --> Parked: Budget guard
    Parked --> Active: Midnight reset
```

Nanoclaw is the early-warning system. It scans token files every 30 minutes and triggers CLIProxyAPI refresh when any token is within 72 hours of expiry. If refresh fails (e.g., revoked grant), it alerts via Telegram and marks the token as requiring manual reauth. The Parked state applies to accounts that hit their daily budget - they come back automatically at midnight, no manual action needed.

---

## 5. Memory Flow

How observations from a session propagate through the memory subsystems and back into future sessions.

```mermaid
graph LR
    S[Session] -->|observations| CM[claude-mem DB]
    S -->|metrics| UD[Usage DB]
    UD -->|hourly| LR[Learning Router]
    LR -->|config| O[Orchestrator]
    S -->|patterns| CO[Coordinator Memory]
    CO -->|daily| AU[Aura Engine]
    AU -->|30min| WS[Workspace CONTEXT.md]
    WS -->|read| S
    CM[claude-mem DB] -->|search| S
```

Sessions write to three targets simultaneously: `claude-mem` for semantic observations, the usage DB for routing metrics, and the coordinator memory DB for task patterns. The learning router reads usage DB metrics hourly and adjusts provider weights. Aura reads coordinator patterns daily and rebuilds the workspace `CONTEXT.md`, which is injected at session start. The cycle is: sessions inform memory, memory informs future sessions.
