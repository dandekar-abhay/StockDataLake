#! /bin/bash

. Stock_Data_Extraction/1_Stock_data.config
index=0;
date_var="$(date +"%Y-%m-%d")"
#echo "Stock data collection started on : $date_var"
#
#echo "No. of companies in the list are:";
#wc -l $FILE_SOURCE
# # just to know the no. of companies in the list
#mkdir ../data/data_"$date_var"
#mkdir ../data/data_"$date_var"/stock_data
#
#touch ../etc/config/date_config.txt
#echo "[date]" > ../etc/config/date_config.txt
#echo "sdate = $date_var" >>../etc/config/date_config.txt
#
#while IFS= read -r value; do
#	index=$(( index +1))   # counter for keeping track of which company data is being gathered
#	echo "gathering $index . $value"
#	python ./Stock_Data_Extraction/zdata.py -i "NSE:$value" -f $F_DATE -t $T_DATE  -n $D_INTERVAL -o ../data/data_"$date_var"/stock_data/$value.csv 	# creates the data file in the pwd with the name of the company in  csv format
##	sleep 1   # sleep command just to avoid blacklisting of my ip address.
#done < $FILE_SOURCE  #file name of list of symbol companies.
#

mkdir ../data/data_"$date_var"/Comp_data
echo $date_var > ./Stock_Data_Extraction/2_logs_"$date_var".txt && python ./Stock_Data_Extraction/2_Comp_data_gather.py >> ./Stock_Data_Extraction/2_logs_"$date_var".txt


