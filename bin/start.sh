#!/usr/bin/env bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
cd $bin


declare -a MAIN_JAR_LOCS=(
  $bin/learning-spark-*-shaded.jar
  $bin/../target/learning-spark-*-shaded.jar
)
MAIN_JAR=''
for x in "${MAIN_JAR_LOCS[@]}"; do
  jar=$(ls $x 2>/dev/null)
  if [[ $? == 0 ]]; then
    MAIN_JAR=$jar
  fi
done
if [[ ! $MAIN_JAR ]]; then
  echo "Main jar not found" && exit 1
else
  echo "Main jar:" $MAIN_JAR
fi

exec /opt/spark/current/bin/spark-submit --class com.example.spark.app.multiline_csv_file ${MAIN_JAR} "$@"