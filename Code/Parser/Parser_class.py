import os
from pyspark.sql import SparkSession
import pandas as pd

class Parser:
    spark = SparkSession.builder \
        .master("local") \
        .appName("parquet_example") \
        .getOrCreate()
# "Parsers Approaches ( short document )
# 1. #Excelsheets
# 2. #XML
# 3. #JSON or JsonLines
# 4. #CSV
# 5. #TSV
# 6. #PDF
# 7. Images data ( Binary data )"




class Csv:
    def __init__(self, file=''):
        self.__file = file

    def convert(self):
        df=pd.read_csv(self.__file)
        parquet=pd.to_parquet(df)
        return parquet

    pass


class Excel:
    def __init__(self, file=''):
        self.__file = file

    def convert(self):
        df = pd.read_excel(self.__file)
        parquet = pd.to_parquet(df)
        return parquet
    pass


class Json:
    def __init__(self, file=''):
        self.__file = file

    def convert(self):
        df = pd.read_json(self.__file)
        parquet = pd.to_parquet(df)
        return parquet
    pass


class Xml:
    def __init__(self, file=''):
        self.__file = file

    def convert(self):
        df = pd.read_xml(self.__file)
        parquet = pd.to_parquet(df)
        return parquet
    pass


class Tsv:
    def __init__(self, file=''):
        self.__file = file

    def convert(self):
        df = pd.read_csv(self.__file, sep='\t')
        parquet = pd.to_parquet(df)
        return parquet
    pass


class Pdf:
    # def __init__(self, file=''):
    #     self.__file = file
    #
    # def convert(self, file=''):
    #     df = pd.read_csv(file)
    #     parquet = pd.to_parquet(df)
    #     return parquet
    # pass

def main():
    path='/home/akash/Desktop/DbdaProject (copy)/PGDBDA-Project/Extract/step1/DataGatherStock/data/'
    entries = os.listdir(path)
    for entry in entries:
        ext = entry.split('.')
        if ext[-1]=='csv':
            o1= Csv(entry)
            x= o1.convert()
        if ext[-1]==('xlsx' or 'xlx'):
            o1= Excel(entry)
            x = o1.convert()
        if ext[-1]==('json' or 'jsonl'):
            o1= Json(entry)
            x = o1.convert()
        if ext[-1]=='xml':
            o1= Xml(entry)
            x = o1.convert()
        if ext[-1]=='tsv':
            o1= Tsv(entry)
            x = o1.convert()
        if ext[-1]=='pdf':
            o1= Pdf(entry)
            x = o1.convert()
        # if ext[-1]=='csv':
        #     o1= CSV()
        # if ext[-1]=='csv':
        #     o1= CSV()





if __name__ == "__main__":
    main()