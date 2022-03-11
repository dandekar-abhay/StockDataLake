import findspark
from configs import Config
from pyspark.sql import SparkSession
from appexceptions import Exceptions


def getSparkSession():
    findspark.init()
    configs = Config.getConfig()
    # TODO: Understand why configs need to be passed again as param to getSparkConfig ?? !!
    sparkAppName = configs.getSparkConfig(configs).get('appName')
    master = configs.getSparkConfig(configs).get('master')
    sparkSession = SparkSession.builder\
        .master(master)\
        .appName(sparkAppName)\
        .getOrCreate()

    if SparkSession.sparkContext:
        print('===============')
        print(f'AppName: {sparkSession.sparkContext.appName}')
        print(f'Master: {sparkSession.sparkContext.master}')
        print('===============')
        return sparkSession
    else:
        print('Could not initialise pyspark session')
        raise Exceptions.InitializeException("Could not init Spark")


# Local tester
if __name__ == '__main__':
    sparkSession1 = getSparkSession()
    print('Spark Master: ' + sparkSession1.sparkContext.master)
    print('Spark AppName: ' + sparkSession1.sparkContext.appName)

