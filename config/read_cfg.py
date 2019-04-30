from configparser import ConfigParser

class ConfigOp:
    def __init__(self):
        # 初始化类
        try:
            self._cp = ConfigParser()
            self._cp.read("../config/pro.cfg")
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


