import os
from configparser import ConfigParser
import structlog
import sys

from crawler.logging_config import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


def generate_env_file():
    config_path = "local.ini"

    if not os.path.exists(config_path):
        logger.critical(
            "local.ini not found. Please ensure it exists in the project root.",
            path=config_path,
        )
        sys.exit(1)

    local_config = ConfigParser()
    try:
        local_config.read(config_path)
    except Exception as e:
        logger.critical(
            "Failed to read local.ini configuration file.",
            path=config_path,
            error=e,
            exc_info=True,
        )
        sys.exit(1)

    # Determine which section to use based on APP_ENV environment variable
    app_env = os.environ.get("APP_ENV", "").upper()

    selected_section_name = "DEFAULT"  # Default fallback
    if app_env and app_env in local_config:
        selected_section_name = app_env
    elif "DEFAULT" not in local_config:
        logger.critical(
            "Neither APP_ENV specified section nor 'DEFAULT' section found in local.ini.",
            app_env=app_env,
        )
        sys.exit(1)

    section = local_config[selected_section_name]
    logger.info("Using configuration section.", section_name=selected_section_name)

    env_content = ""
    for key, value in section.items():
        env_content += f"{key.upper()}={value}\n"

    env_file_path = ".env"
    try:
        with open(env_file_path, "w", encoding="utf8") as env_file:
            env_file.write(env_content)
        logger.info(
            ".env file generated successfully.",
            path=env_file_path,
            section_used=selected_section_name,
        )
    except Exception as e:
        logger.critical(
            "Failed to write .env file.", path=env_file_path, error=e, exc_info=True
        )
        sys.exit(1)


if __name__ == "__main__":
    generate_env_file()
