import os
import snowflake.connector

class Snowflake:
    def __init__(self):

        self.snl_account = os.getenv("SNOWFLAKE_ACCOUNT", "fake")
        self.snl_database = os.getenv("SNOWFLAKE_DB", "fake")
        self.snl_schema = os.getenv("SNOWFLAKE_SCHEMA", "fake")
        self.snl_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "fake")
        self.snl_role = os.getenv("SNOWFLAKE_ROLE", "fake")
        self.snl_user = os.getenv("SNOWFLAKE_USERNAME", "fake")
        self.snl_password = os.getenv("SNOWFLAKE_PASSWORD", "fake")

    def conn(_self):

        return snowflake.connector.connect(
            user=_self.snl_user,
            password=_self.snl_password,
            account=_self.snl_account,
            role=_self.snl_role,
            database=_self.snl_database,
            warehouse=_self.snl_warehouse,
        )

