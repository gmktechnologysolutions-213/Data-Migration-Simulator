from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class MySQLConfig:
    host: str = os.getenv("MYSQL_HOST", "localhost")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    user: str = os.getenv("MYSQL_USER", "root")
    password: str = os.getenv("MYSQL_PASSWORD", "")
    database: str = os.getenv("MYSQL_DATABASE", "source_db")

@dataclass
class SnowflakeConfig:
    user: str = os.getenv("SNOWFLAKE_USER", "")
    password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database: str = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS")
    schema: str = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    role: str = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

@dataclass
class AppConfig:
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
