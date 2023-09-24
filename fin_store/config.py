from os import path, environ
from pathlib import Path
from configparser import ConfigParser
from fin_store.logger import write_log


class IniConfig(object):

    def __init__(self, name, paths, encoding='utf-8'):
        parser = ConfigParser()
        paths_ = [path.join(p, name) for p in paths]
        parser.read(paths_, encoding)
        env = environ
        write_log('read configs ', paths_)
        sections = dict()
        for section in parser.sections():
            params = dict(parser.items(section))
            prefix = '{}_'.format(section.upper())
            props = [(key.removeprefix(prefix).lower(), env[key]) for key in env if key.startswith(prefix)]
            params.update([(k, v) for (k, v) in props if k in params.keys()])
            sections[section] = params
        self.sections = sections

    def get_section(self, *sections):
        res = dict()
        names = list(sections)
        names.reverse()
        [res.update(self.sections.get(n, dict())) for n in names]
        return res

    def get_property(self, name, default=None, *sections):
        return self.get_section(*sections).get(name, default)


def get_section(*section):
    return config.get_section(*section)


def get_property(name, *section, default=None):
    return config.get_property(name, default, *section)


def reload_config():
    global config
    print([
        path.dirname(path.realpath(__file__)),
        str(Path.cwd()),
        str(Path.home())
    ])
    config = IniConfig(name='config.ini', paths=[
        path.dirname(path.realpath(__file__)),
        str(Path.cwd()),
        str(Path.home())
    ])


reload_config()
