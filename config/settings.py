from pathlib import Path
from typing import ClassVar
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    BASE_DIR: ClassVar[Path] = BASE_DIR

    DATA_GENERATOR_CONFIG_PATH: str = str(
        BASE_DIR / "a_data_generator" / "config" / "generator_config.yaml"
    )
    TEST_DATA_GENERATOR_CONFIG_PATH: str = str(
        BASE_DIR / "tests" / "a_data_generator" / "test_config.yaml"
    )
    DATA_GENERATOR_OUTPUT_PATH: str = str(
        BASE_DIR / "a_data_generator" / "outputs"
    )

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
