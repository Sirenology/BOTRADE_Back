import configparser


class ConfigManager:
    def __init__(self, config_file='D:\\WorkSpace\\Graduation_Design\\SEBOTRADE\\BOTRADE_Back\\botrade_back\\base\\config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get(self, section, option):
        return self.config.get(section, option)
