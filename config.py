import yaml
import os


class Config:
    def __init__(self, path):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.safe_load(yaml_file)

    def get_config(self):
        return self.__config



