#! /bin/bash
date_var="$(date +"%Y-%m-%d")"

echo "Install required Packages?(Y/N)"
read choice1
if [ $choice1 == "Y" ]
then
  pip install -r ../requirements.txt
fi
echo
echo "Provide Zerodha Login details:"
jtrader zerodha startsession

echo "Collecting data now ...."
bash ./Stock_Data_Extraction/1_BASHStockScript.sh

sleep 4s

echo "Performing File Handling operations now ...."
echo "To proceed further make sure to 'change the renaming files config files containing paths to respective folders'. And then"
read -p "Press any key to continue... " -n 1 -s


mkdir ../data/data_"$date_var"/scriptnamedata
#hdfs dfs -mkdir ./data_"$date_var"

python ./File_handling/renaming_files.py

echo "Choose the pattern recognition technique to follow:
    1. The three white soldiers and the three black crows candlestick patterns

    Enter the choice number you want to perform:  "
read choice3
touch ../Spark_window_pattern_recognition/Analysis_Output_"$date_var".txt
if [ $choice3 == "1" ]
then
  python ../Spark_window_pattern_recognition/Candle_pattern_prog.py  | tee -a ../Spark_window_pattern_recognition/Analysis_Output_"$date_var".txt
  counter=$(( counter +1))
  echo "------------------------------------------------------------------------------------------------------">>../Spark_window_pattern_recognition/Analysis_Output_"$date_var".txt
  echo " NEXT OUTPUT |  NEXT OUTPUT |  NEXT OUTPUT |  NEXT OUTPUT |  NEXT OUTPUT |  NEXT OUTPUT |  NEXT OUTPUT">>../Spark_window_pattern_recognition/Analysis_Output_"$date_var".txt
  echo "------------------------------------------------------------------------------------------------------">>../Spark_window_pattern_recognition/Analysis_Output_"$date_var".txt

fi

echo