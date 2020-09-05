from src import *
from src.utils.jdbc import write_jdbc_table

_spark = get_spark()


def write_jdbc(sql_script, jdbc_table):
    logger.info("Load Staging from {0} to {1} started.".format(sql_script, jdbc_table))
    sql_path = APPLICATION_PATH + SQL_PATH

    with open(sql_path + sql_script) as fr:
        file_content = fr.read()
        query = file_content.format(STAGING_SCHEMA)

        df_sql = _spark.sql(query)
        write_jdbc_table(df_sql, jdbc_table)
        logger.info("Write data to {0} finished.".format(jdbc_table))

