import inspect
import types
from .delta import autogenerate

__all__ = ["aliases", "expose", "make_callable", "HiiSpiderPlugin", "delta"]
EXPOSED_FUNCTIONS = {}
CALLABLE_FUNCTIONS = {}
MEMOIZED_FUNCTIONS = {}
FUNCTION_ALIASES = {}
DELTA_FUNCTIONS = {}


def aliases(*args):
    def decorator(f):
        FUNCTION_ALIASES[id(f)] = args
        return f
    return decorator


def autodelta(func):
    DELTA_FUNCTIONS[id(func)] = autogenerate
    return func

def delta(handler):
    def decorator(f):
        DELTA_FUNCTIONS[id(f)] = handler
        return f
    return decorator


def expose(func=None, interval=0, category=None, name=None, memoize=False):
    if func is not None:
        EXPOSED_FUNCTIONS[id(func)] = {"interval":interval, "name":name, 'category':category}
        return func
    def decorator(f):
        EXPOSED_FUNCTIONS[id(f)] = {"interval":interval, "name":name, 'category':category}
        return f
    return decorator


def make_callable(func=None, interval=0, category=None, name=None, memoize=False):
    if func is not None:
        CALLABLE_FUNCTIONS[id(func)] = {"interval":interval, "name":name, 'category':category}
        return func
    def decorator(f):
        CALLABLE_FUNCTIONS[id(f)] = {"interval":interval, "name":name, 'category':category}
        return f
    return decorator


class HiiSpiderPlugin(object):

    def __init__(self, spider):
        self.spider = spider
        check_method = lambda x:isinstance(x[1], types.MethodType)
        instance_methods = filter(check_method, inspect.getmembers(self))
        for instance_method in instance_methods:
            instance_id = id(instance_method[1].__func__)
            if instance_id in DELTA_FUNCTIONS:
                self.spider.delta(
                    instance_method[1],
                    DELTA_FUNCTIONS[instance_id])
            if instance_id in EXPOSED_FUNCTIONS:
                self.spider.expose(
                    instance_method[1],
                    interval=EXPOSED_FUNCTIONS[instance_id]["interval"],
                    name=EXPOSED_FUNCTIONS[instance_id]["name"],
                    category=EXPOSED_FUNCTIONS[instance_id]["category"])
                if instance_id in FUNCTION_ALIASES:
                    for name in FUNCTION_ALIASES[instance_id]:
                        self.spider.expose(
                            instance_method[1],
                            interval=EXPOSED_FUNCTIONS[instance_id]["interval"],
                            name=name)
            if instance_id in CALLABLE_FUNCTIONS:
                self.spider.expose(
                    instance_method[1],
                    interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                    name=CALLABLE_FUNCTIONS[instance_id]["name"],
                    category=CALLABLE_FUNCTIONS[instance_id]["category"])
                if instance_id in FUNCTION_ALIASES:
                    for name in CALLABLE_FUNCTIONS[instance_id]:
                        self.spider.expose(
                            instance_method[1],
                            interval=CALLABLE_FUNCTIONS[instance_id]["interval"],
                            name=name)

    def setFastCache(self, uuid, data):
        return self.spider.setFastCache(uuid, data)

    def getPage(self, *args, **kwargs):
        return self.spider.getPage(*args, **kwargs)

