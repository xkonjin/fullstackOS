from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Linear
    linear_api_key: str = Field(default="", alias="LINEAR_API_KEY")
    linear_webhook_secret: str = Field(default="", alias="LINEAR_WEBHOOK_SECRET")
    linear_trigger_status: str = Field(default="AI Queue", alias="LINEAR_TRIGGER_STATUS")
    linear_in_review_status: str = Field(default="In Review", alias="LINEAR_IN_REVIEW_STATUS")
    linear_deploy_status: str = Field(default="Deploy", alias="LINEAR_DEPLOY_STATUS")

    # CLIProxyAPI
    cliproxyapi_url: str = Field(default="http://127.0.0.1:8317/v1", alias="CLIPROXYAPI_URL")
    cliproxyapi_key: str = Field(default="your-proxy-key", alias="CLIPROXYAPI_KEY")

    # Agent Gateway
    agent_gateway_url: str = Field(default="http://127.0.0.1:18789", alias="AGENT_GATEWAY_URL")

    # Messaging Agent
    messaging_url: str = Field(default="http://127.0.0.1:8317/v1", alias="MESSAGING_URL")
    messaging-agent_model: str = Field(default="gpt-4.1", alias="MESSAGING_MODEL")

    # Telegram
    messaging-agent_mini_telegram_bot_token: str = Field(default="", alias="MESSAGING_TELEGRAM_BOT_TOKEN")
    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    telegram_owner_chat_id: str = Field(default="", alias="TELEGRAM_OWNER_CHAT_ID")

    @property
    def active_telegram_bot_token(self) -> str:
        if self.messaging-agent_mini_telegram_bot_token:
            return self.messaging-agent_mini_telegram_bot_token
        if self.telegram_bot_token:
            import logging
            logging.getLogger(__name__).warning(
                "MESSAGING_TELEGRAM_BOT_TOKEN not set — falling back to TELEGRAM_BOT_TOKEN"
            )
            return self.telegram_bot_token
        return ""

    # Symphony
    symphony_port: int = Field(default=8400, alias="SYMPHONY_PORT")
    default_model: str = Field(default="gpt-5.4", alias="DEFAULT_MODEL")
    triage_model: str = Field(default="gpt-5.4", alias="TRIAGE_MODEL")

    # Approval gate
    approval_timeout_minutes: int = Field(default=60, alias="APPROVAL_TIMEOUT_MINUTES")

    # Repo allowlist — only process issues with these labels (repo names)
    # Comma-separated list of label names that map to repos the user owns
    allowed_repo_labels: str = Field(
        default="webapp,api-server,support-ai,dashboard,prototype,mobile-app,marketing-site,infrastructure",
        alias="ALLOWED_REPO_LABELS",
    )

    @property
    def allowed_labels_set(self) -> set[str]:
        return {l.strip() for l in self.allowed_repo_labels.split(",") if l.strip()}


settings = Settings()
