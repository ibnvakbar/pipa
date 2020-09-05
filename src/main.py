from src import *
from src.module.ods2eds import load_ods_to_eds
from src.module.ods2staging import load_staging
from src.module.staging2jdbc import write_jdbc

MODULE = sys.argv[1]
SQL = sys.argv[2]
TABLE = sys.argv[3]
BUSS_DATE = sys.argv[4]


def run():
    """
    Main method
    :return:
    """
    logger = Logger()
    logger.info("Module {} Start".format(MODULE))

    # Call process
    if MODULE.lower() == "ods2eds":
        load_ods_to_eds(SQL, TABLE, BUSS_DATE)

    elif MODULE.lower() == "ods2staging":
        load_staging(SQL, TABLE, BUSS_DATE)

    elif MODULE.lower() == "staging2jdbc":
        write_jdbc(SQL, TABLE)
