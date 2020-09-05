from src import *

_spark = get_spark()


def load_staging(ods_script, staging_table, buss_date):
    logger.info("Load Staging from {0} to {1}.{2} started.".format(ods_script, STAGING_SCHEMA, staging_table))
    sql_path = APPLICATION_PATH + SQL_PATH

    with open(sql_path + ods_script) as fr:
        file_content = fr.read()
        query = file_content.format(ODS_SCHEMA)
        if buss_date != '0':
            query += " AND BUSINESS_DATE = '{}' ".format(buss_date)

        df_sql = _spark.sql(query)
        df_sql.createOrReplaceTempView('{0}'.format(staging_table))

        table_list = _spark.sql("show tables in {0}".format(STAGING_SCHEMA))
        table_name = table_list.filter(table_list.tableName == "{0}".format(staging_table)).collect()
        if len(table_name) > 0:
            _spark.sql('''
                INSERT OVERWRITE TABLE {0}.{1}
                SELECT * FROM {1}
            '''.format(STAGING_SCHEMA, staging_table))
        else:
            _spark.sql('''
                CREATE TABLE {0}.{1} AS 
                SELECT * FROM {1}
            '''.format(STAGING_SCHEMA, staging_table))

    logger.info("Write data to {0}.{1} finished.".format(ODS_SCHEMA, staging_table))

