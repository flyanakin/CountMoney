import pandas as pd
from sqlalchemy import create_engine
from dagster import IOManager, io_manager, Field, OutputContext, AssetKey, StringSource


class PandasSqlIOManager(IOManager):
    DEFAULT_WRITE_MODE = "append"

    def __init__(
        self,
        hosts,
        user,
        secret,
        database,
        schema,
        write_mode: str = DEFAULT_WRITE_MODE,
    ):
        """
        通过pandas输出SQL到RDB数据库，默认输出到PG，后续再考虑兼容其他
        :param database:pg数据库名称
        :param schema:pg schema名称
        :param user:pg 数据库账号
        :param secret:pg 数据库密码
        :param write_mode:写入模式，默认append
        """
        self.__hosts = hosts
        self.__database = database
        self.__schema = schema
        self.__user = user
        self.__secret = secret
        if write_mode in ["append", "replace"]:
            self.__write_mode = write_mode
        else:
            raise ValueError(
                "Unsupported PandasSqlIOManager write mode: " + str(write_mode)
            )

    def handle_output(self, context: "OutputContext", df) -> None:
        engine = create_engine(
            f"postgresql://{self.__user}:{self.__secret}@{self.__hosts}/{self.__database}"
        )
        df.to_sql(
            name=context.asset_key[0][0],
            con=engine,
            index=False,
            schema=self.__schema,
            if_exists=self.__write_mode,
            chunksize=5000,
        )

    def load_input(self, context: "InputContext"):
        """
        暂时不需要pandas读数据，先不实现
        :param context:
        :return:
        """
        pass


@io_manager(
    config_schema={
        "warehouse_hosts": Field(StringSource, is_required=True),
        "warehouse_user": Field(StringSource, is_required=True),
        "warehouse_secret": Field(StringSource, is_required=True),
        "destination_db": Field(StringSource, is_required=True),
        "destination_schema": Field(StringSource, is_required=True),
    },
)
def pandas_sql_append_io_manager(init_context):
    return PandasSqlIOManager(
        hosts=init_context.resource_config.get("warehouse_hosts"),
        user=init_context.resource_config.get("warehouse_user"),
        secret=init_context.resource_config.get("warehouse_secret"),
        database=init_context.resource_config.get("destination_db"),
        schema=init_context.resource_config.get("destination_schema"),
        write_mode="append",
    )


@io_manager(
    config_schema={
        "warehouse_hosts": Field(StringSource, is_required=True),
        "warehouse_user": Field(StringSource, is_required=True),
        "warehouse_secret": Field(StringSource, is_required=True),
        "destination_db": Field(StringSource, is_required=True),
        "destination_schema": Field(StringSource, is_required=True),
    },
)
def pandas_sql_replace_io_manager(init_context):
    return PandasSqlIOManager(
        hosts=init_context.resource_config.get("warehouse_hosts"),
        user=init_context.resource_config.get("warehouse_user"),
        secret=init_context.resource_config.get("warehouse_secret"),
        database=init_context.resource_config.get("destination_db"),
        schema=init_context.resource_config.get("destination_schema"),
        write_mode="replace",
    )
