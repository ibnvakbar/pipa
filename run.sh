#!/bin/bash
# shellcheck disable=SC2086

source run.properties

# List Parameter Variable
MODULE=
SQL=
TABLE=
BUSSDATE=

usage="Penggunaan: $(basename "$0") [-h] [-m <module>] [-q <sql>] [-t <table>] [-d <date>]

Parameter:
    -h  untuk menunjukkan help text ini
    -m  set module name (ods2eds, ods2staging, staging2jdbc)
    -q  set sql file yang berisi source query (cth: customer.sql)
    -t  set target table name (cth: CUSTOMER)
    -d  set business date (format: yyyy-MM-dd; 0 for not using bussiness date)

"

while getopts ':hm:q:t:d:' option; do
  case "$option" in
    h) printf "Ini adalah Spark program untuk load data dari ODS ke EDS.\n\n%s" "$usage"
       exit
       ;;
    m) MODULE=$OPTARG
       ;;
    q) SQL=$OPTARG
       ;;
    t) TABLE=$OPTARG
       ;;
    d) BUSSDATE=$OPTARG
       ;;
    :) printf "Parameter [-%s] belum diisi value-nya.\n\n" "$OPTARG" >&2
       echo "$usage" >&2
       exit 1
       ;;
   \?) printf "Hellooo... siapa yang suruh pake parameter [-%s]??\n\n" "$OPTARG" >&2
       echo "$usage" >&2
       exit 1
       ;;
  esac
done
shift $((OPTIND - 1))

if [[ -z $SQL ]] || [[ -z $TABLE ]] || [[ -z $BUSSDATE ]] || [[ -z $MODULE ]]
then
    printf "Yakin ga ada parameter yang ketinggalan?? \n\n%s" "$usage"
else
    APP_NAME="[BATCH] NET Load ${SQL} to ${TABLE}"
    PYSPARK_PYTHON=$PYTHON_PATH/python \
    PYSPARK_DRIVER_PYTHON=$PYTHON_PATH/python \
    $SPARK_PATH/spark-submit \
      --master yarn \
      --conf "spark.app.name=$APP_NAME" \
      --conf "spark.master=$SPARK_MASTER" \
      --conf "spark.executor.cores=$SPARK_EXECUTOR_CORES" \
      --conf "spark.driver.memory=$SPARK_DRIVER_MEMORY" \
      --conf "spark.executor.memory=$SPARK_EXECUTOR_MEMORY" \
      --conf "spark.executor.instances=$SPARK_EXECUTOR_INSTANCES" \
      --conf "spark.driver.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client-spark.jar:/etc/hbase/conf/ " \
      --conf "spark.executor.extraClassPath=/usr/hdp/current/phoenix-client/phoenix-client-spark.jar:/etc/hbase/conf/" \
      --conf "spark.ui.enabled=false" \
      --jars $JARS \
      --py-files $PROJECT_DIR/src.zip \
      $PROJECT_DIR/run.py $MODULE $SQL $TABLE $BUSSDATE
fi
