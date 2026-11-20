# app/config.py
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SUPPORTED_LLM_PROVIDERS = {"openai", "anthropic", "gemini"}
OPENAI_COMPAT_ALIASES = {"bigmodel", "zhipu", "zai"}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "antifraud"
    postgres_db: str | None = None
    postgres_user: str = "antifraud"
    postgres_password: str = "antifraud"
    postgres_schema: str = "public"

    # Memgraph
    memgraph_url: str = "bolt://localhost:7687"
    memgraph_username: str = ""
    memgraph_password: str = ""

    # Migration / Graph Config
    migration_strategy: str = "deterministic"
    graph_config_path: str = "config/antifraud_graph.yaml"

    # LLM (only required when migration_strategy == "llm")
    openai_api_key: str = ""
    llm_api_key: str = ""
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o-mini"
    llm_base_url: str = ""
    openai_base_url: str = ""

    @model_validator(mode="after")
    def _normalize_llm_settings(self) -> "Settings":
        self.migration_strategy = (self.migration_strategy or "deterministic").strip().lower()

        provider = (self.llm_provider or "openai").strip().lower()
        if provider in OPENAI_COMPAT_ALIASES:
            self.llm_provider = "openai"
        else:
            self.llm_provider = provider

        if not self.llm_base_url and self.openai_base_url:
            self.llm_base_url = self.openai_base_url

        if self.llm_provider == "openai" and self.llm_model.startswith("bigmodel/"):
            self.llm_model = self.llm_model.removeprefix("bigmodel/")

        return self

    @property
    def db_name(self) -> str:
        return self.postgres_db or self.postgres_database

    @property
    def effective_llm_key(self) -> str:
        return self.openai_api_key or self.llm_api_key


settings = Settings()