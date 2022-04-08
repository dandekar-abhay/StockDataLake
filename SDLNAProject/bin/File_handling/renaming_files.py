import csv
import os
import pandas as pd
import xlrd
from pyspark import SparkConf
from xlrd import XLRDError
from fuzzywuzzy import process
import openpyxl
import Levenshtein
import os
import subprocess
import csv
import re
import pyspark.pandas as ps
import configparser
import gc
#conf = SparkConf().setAppName("RatingsHistogram").setMaster("local")
#export JVM_ARGS="-Xms1024m -Xmx1024m"

import pyspark
from delta import *
#
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "6G") \
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.kryoserializer.buffer.max", "2000M")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

def excel_rename(path):
    for filename in os.listdir(path):
        try:
            filepath = os.path.join(path, filename)
            excel_file = xlrd.open_workbook(filepath)
            worpsheet = excel_file.sheet_by_index(5)
            new_filename = worpsheet.cell_value(0, 1)
            x = new_filename.title()
            m = x.split(' ')
            print(m)
            m = list(map(lambda y: y.replace('Ltd', 'Limited'), m))
            j = (' ').join(m)
            print(j)
            os.rename(filepath, os.path.join(path, j + '.xlsx'))
        except XLRDError:
            print(" renaming failed")
            continue


def excel_rename_symbol(path, newpath, scriptpath):
    data = pd.read_csv(f"{scriptpath}")
    # print(data)
    i = 0
    IDs = {}
    with open(scriptpath) as f:
        data = csv.reader(f)
        for row in data:
            k = row[1].lower()
            IDs[k] = row[0]

    for filename in os.listdir(path):
        print(filename)
        try:
            filepath = os.path.join(path, filename)
            x = os.path.splitext(filename.lower())
            print(x)
            os.rename(filepath, os.path.join(newpath, f'{IDs[x[0]]}.xlsx'))
        except KeyError:
            i = i + 1
            # print(f'{i}.{filename} Reaname failed')
            filepath = os.path.join(path, filename)
            x = os.path.splitext(filename.lower())
            print(x[0] + str(i) + ": FAILED BUT RETRYING USING BEST MATCH ")
            keys_list = list(IDs.keys())
            query = x[0]
            Best_match, perc = process.extractOne(query, keys_list)
            os.rename(filepath, os.path.join(newpath, f'{IDs[Best_match]}.xlsx'))
            print(str(i) + ": Changed " + x[0] + " Sucessfully using best match with " + str(perc) + "% match")


def xlsxtodeltaextraction(path, newPath, pathToStockData):


    path_now = os.getcwd()
    print(path_now)
    os.chdir(pathToStockData)
    abs_path_stockdata = os.getcwd()
    os.chdir(path_now)
    os.chdir(path)
    abs_path_compda = os.getcwd()
    os.chdir(path_now)

    counter = 0
    for filename in os.listdir(path):
        print(filename)
        x = os.path.splitext(filename)
        print(x)
        counter +=1



        try:
            print(f"file:///{abs_path_stockdata}/{x[0]}.csv")
            # str = f"file:///{abs_path_stockdata}/{x[0]}.csv"
            ps.set_option('compute.ops_on_diff_frames', True)
            df_comp = ps.read_csv(f'File:///{abs_path_stockdata}/{x[0]}.csv')

            df_comp['Company_Name'] = x[0]
            # df1 = ps.DataFrame()
            df1 = df_comp['close']
            df_comp['Trend_close'] = df1.diff()
            df_comp.loc[df_comp['Trend_close'] > 0, 'Day_Trend'] = 'up'
            df_comp.loc[df_comp['Trend_close'] < 0, 'Day_Trend'] = 'down'
            df_comp.loc[df_comp['Trend_close'] == 0, 'Day_Trend'] = 'stable'
            df_comp.loc[1, ['Day_Trend']] = 'stable'
            df_comp.fillna(0, inplace=True)

            df_comp.to_spark_io(f'{newPath}/stock_data/',mode="append",partition_cols="Company_Name", format="delta")  # stockdata file

        except:
            continue

        df = ps.read_excel(
            f'file:///{abs_path_compda}/{x[0]}.xlsx',
            sheet_name='Data Sheet', header=None, skiprows=16, nrows=77,
            dtype={0: str, 1: str, 2: str, 3: str, 4: str, 5: str, 6: str, 7: str, 8: str, 9: str, 10: str})
        ps.set_option('compute.ops_on_diff_frames', True)
        l1 = [int(x) for x in range(0, 15)]  # polo
        l2 = [int(x) for x in range(40, 56)]  # bal
        l3 = [int(x) for x in range(65, 69)]  # cash
        l1.append(76)
        l1.extend(l2)
        l1.extend(l3)
        print("Comp_data" + x[0])
        df = df.loc[l1]
        df.fillna(0.0, inplace=True)
        df[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]] = df[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]].apply(ps.to_numeric)
        names = ['Entities', 'Mar_12', 'Mar_13', 'Mar_14', 'Mar_15', 'Mar_16', 'Mar_17', 'Mar_18', 'Mar_19', 'Mar_20',
                 'Mar_21']
        df.columns = names
        df['Company_Name'] = x[0]
        df.to_spark_io(
            f'{newPath}/comp_data/',mode="append",partition_cols="Company_Name",  format="delta")



if __name__ == '__main__':
    abs_path = os.path.dirname(os.path.abspath(__file__))
    print(os.getcwd())

    os.chdir(f'{abs_path}/../../etc/config/')
    config = configparser.ConfigParser()
    config.read('renamingfiles_config.ini')
    os.chdir(f'{abs_path}')
    # path1 = config.get('excel_rename','path1')
    #
    # excel_rename(path1)   #Function used to rename the files according to company name   [LOCAL]
    #
    # path2 = config.get("excel_rename_symbol", "path2")
    # path3 = config.get("excel_rename_symbol", "path3")
    # path4 = config.get("excel_rename_symbol", "path4")
    #
    # excel_rename_symbol(path2, path3, path4)  #Function used to rename the excel file to their respective script names  [LOCAL]

    path5 = config.get("xlsxtodeltaextraction", "path5")
    path6 = config.get("xlsxtodeltaextraction", "path6")
    path7 = config.get("xlsxtodeltaextraction", "path7")

    xlsxtodeltaextraction(path5, path6, path7)  # Function used to extract tables from excel screener data.    [HDFS]

    print("-------------------- ALL FILES EXTRACTION DONE -------------------------")
    input("Press enter to terminate")
    spark.stop()
