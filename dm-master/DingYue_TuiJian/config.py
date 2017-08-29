# coding=utf-8
import os
import ConfigParser
from tornado.log import gen_log
from dotdict import DotDict

Config = DotDict()

REDIS_POOL = None


def load_file(file_path=None):
    if not file_path:
        return
    configParse = ConfigParser.ConfigParser()
    try:
        configParse.read(file_path)
    except Exception:
        gen_log.warning("load_file %s error", file_path)
        raise

    d = {}
    for section in configParse.sections():
        d[section] = DotDict(dict([(x, unicode(y, "utf-8")) for x, y in configParse.items(section)]))

    return d


def load(path="config-line"):
    for fn in os.listdir(path):
        fname = os.path.join(path, fn)
        if os.path.isdir(fname) or fname.endswith(".py"):  # 跳过目录和python文件
            continue
        Config.update(load_file(fname))
    return Config


if __name__ == "__main__":
    load("config-line")
    print Config.flatten()
    print "type: %s" % type(Config)
