#! /bin/bash

. Stock_Data_Extraction/1_Stock_data.config

date_var="$(date +"%Y-%m-%d")"

touch ../etc/config/date_config.txt
echo "[date]" > ../etc/config/date_config.txt
echo "sdate = $date_var" >>../etc/config/date_config.txt


touch ../etc/config/DATA_COLL_OPT.ini
echo "[OPT]" > ../etc/config/DATA_COLL_OPT.ini

echo "Choose from below options:
    1. Collect only stock data - holc volume - stock market related - zerodha
    2. Collect only Company data - bal sht, pro, cash flow  - screener
    3. Collect both stock & company data - 1& 2
    Make sure to change required variables in 1_Stock_data.config file
    Enter the choice number you want to perform:  "

read -n 1 choice2

case $choice2 in
  1)
    echo "option = 1" >>  ../etc/config/DATA_COLL_OPT.ini
    index=0;
    echo "Stock data collection started on : $date_var"

    echo "No. of companies in the list are:";
    wc -l $FILE_SOURCE     # wc -l ./Stock_Data_Extraction/1_scriptstock.csv
     # just to know the no. of companies in the list
    mkdir ../data/data_"$date_var"
    mkdir ../data/data_"$date_var"/stock_data

counter=0

  while IFS= read -r value; do    # Internal field separator
    index=$(( index +1))
       # counter for keeping track of which company data is being gathered
    echo "gathering $index . $value"
    python ./Stock_Data_Extraction/zdata.py -i "NSE:$value" -f $F_DATE -t $T_DATE  -n $D_INTERVAL -o ../data/data_"$date_var"/stock_data/$value.csv && 	counter=$(( counter +1))
     # creates the data file in the pwd with the name of the company in  csv format
  done < $FILE_SOURCE  #file name of list of symbol companies.
echo " No. of Files succesfully gathered : $counter"

  ;;
  2)
    echo "option = 2" >>  ../etc/config/DATA_COLL_OPT.ini
    mkdir ../data/data_"$date_var"/Comp_data
    echo $date_var > ./Stock_Data_Extraction/2_logs_"$date_var".txt && python ./Stock_Data_Extraction/2_Comp_data_gather.py >> ./Stock_Data_Extraction/2_logs_"$date_var".txt
  ;;

  3)
    echo "option = 3" >>  ../etc/config/DATA_COLL_OPT.ini
    index=0;
    echo "Stock data collection started on : $date_var"

    echo "No. of companies in the list are:";
    wc -l $FILE_SOURCE
     # just to know the no. of companies in the list
    mkdir ../data/data_"$date_var"
    mkdir ../data/data_"$date_var"/stock_data



    while IFS= read -r value; do
      index=$(( index +1))   # counter for keeping track of which company data is being gathered
      echo "gathering $index . $value"
      python ./Stock_Data_Extraction/zdata.py -i "NSE:$value" -f $F_DATE -t $T_DATE  -n $D_INTERVAL -o ../data/data_"$date_var"/stock_data/$value.csv 	# creates the data file in the pwd with the name of the company in  csv format
    #	sleep 1   # sleep command just to avoid blacklisting of my ip address.
    done < $FILE_SOURCE  #file name of list of symbol companies.

    mkdir ../data/data_"$date_var"/Comp_data
    echo $date_var > ./Stock_Data_Extraction/2_logs_"$date_var".txt && python ./Stock_Data_Extraction/2_Comp_data_gather.py >> ./Stock_Data_Extraction/2_logs_"$date_var".txt


esac




