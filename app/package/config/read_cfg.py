from configparser import ConfigParser
import os
import logging
class ConfigOp:
    def __init__(self):
        # 初始化类
        try:
            self._cp = ConfigParser()
            cfg_file="./package/config/pro.cfg"
            if os.path.exists(cfg_file):
                self._cp.read(cfg_file)
            else:
                cfg_file = "/trade/app/package/config/pro.cfg"
                self._cp.read(cfg_file)
            logging.info("配置文件路径：" + cfg_file)
        except Exception as e:
            print(u'__init__ 失败.'+__file__, e)

    def get_config(self, section, option):
        if section != '':
            _option_val = self._cp.get(section, option)
            return _option_val
        return ''

    def get_mongocfg(self,section):
        _options=self._cp.items(section)
        _ip=self.get_config(section,'ip')
        _port=int(self.get_config(section,'port'))
        return _ip,_port


