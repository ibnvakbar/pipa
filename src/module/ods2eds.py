from src import *
from src.utils.phoenix import write_phoenix_table

_spark = get_spark()


def load_ods_to_eds(ods_script, eds_table, buss_date):
    logger.info("ODS load {0} to {1}.{2} started.".format(ods_script, EDS_SCHEMA, eds_table))
    sql_path = APPLICATION_PATH + SQL_PATH

    with open(sql_path + ods_script) as fr:
        file_content = fr.read()
        query = file_content.format(ODS_SCHEMA)
        if buss_date != '0':
            query += " AND BUSINESS_DATE = '{}' ".format(buss_date)

        df_sql = _spark.sql(query)
        write_phoenix_table(df_sql, '{0}.{1}'.format(EDS_SCHEMA, eds_table))
        logger.info("Write data to {0} finished.".format(eds_table))
