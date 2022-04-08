#! /bin/bash
date_var="$(date +"%Y-%m-%d")"
pip install jugaad-trader
jtrader zerodha startsession

sleep 4s
echo "Collecting Stock data now ...."
bash ./Stock_Data_Extraction/1_BASHStockScript.sh
echo "Performing File Handling operations now ...."
echo "Enter 'proceed in Uppercase' to proceed further after 'changing the renaming files config files containing paths to respective folders'."
read PASSPHRASE
while [ $PASSPHRASE != "PROCEED" ]; do
  echo "Passphrase not entered correctly"
  echo "Enter 'proceed in Uppercase' to proceed further after 'changing the renaming files config files containing paths to respective folders'."
  read PASSPHRASE
done

mkdir ../data/scriptnamedata_"$date_var"
hdfs dfs -mkdir ./data_"$date_var"

python ./File_handling/renaming_files.py

