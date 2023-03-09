#!/bin/bash

# set folder path
csv_folder_path="raw_csvs"
sql_folder_path="models/load_activities"

# loop through all csv files in folder
for file in "$csv_folder_path"/*.csv;
do
  # generate sql file with same name
  filename=$(basename -s .csv "$file").sql
  cat > "$sql_folder_path"/"$filename" <<EOF
{{
  config(materialized = 'table')
}}
select *
from read_csv_auto('$file')
EOF
done