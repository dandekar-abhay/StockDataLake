# currentConfig = "S3"
from configs.LsacAuth import *
import logging
from appexceptions.Exceptions import ShouldNotBeCalledException, InvalidArgumentException

currentConfig = "local"
logging.basicConfig(filename='/tmp/sdl.log', level=logging.INFO)


def getConfig():
    if currentConfig == "S3":
        return S3Config
    elif currentConfig == "local":
        return LocalConfig


class Config:

    def __init__(self):
        print("init")

    def getStorageConfig(self):
        print("This is the base class config")
        raise ShouldNotBeCalledException()

    def getDBConfig(self):
        print("This is the base class config")
        raise ShouldNotBeCalledException()

    def getSparkConfig(self):
        print("This is the base class config")
        raise ShouldNotBeCalledException()


'''
    Class to provide local config
'''


class LocalConfig(Config):

    def __init__(self):
        print("init")

    storageConfig = {

    }

    sparkConfig = {'master': 'local',
                   'appName': 'SDL On Local'}

    def getDBConfig(self):
        return {}

    def getStorageConfig(self):
        return self.storageConfig

    def getSparkConfig(self):
        return self.sparkConfig


# TODO: Implement a config for S3
class S3Config(Config):

    def getStorageConfig(self):
        print("This is the S3 class config")

    def getDBConfig(self):
        print("This is the S3 class config")

    def getSparkConfig(self):
        print("This is the S3 class config")

    def __init__(self):
        storageConfig = getStorageConfig()


# Local test
if __name__ == '__main__':
    config = getConfig()
