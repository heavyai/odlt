import json
from functools import partial

def is_json(string):
    """
    Helper function to determine if a string is valid JSON
    """
    try:
        json_object = json.loads(string)
    except ValueError as e:
        return False
    return True

class validate_connection(object):
    def __init__(self, func):
        self.func = func
        self.name = func.__name__
    
    def __call__(self, instance, *args, **kwargs):
        if not instance._conn:
            raise ValueError('No OmniSci connection has been made, please pass a connection object when initializing the object, or use the connect method')
        ret = self.func(instance, *args, **kwargs)
        return ret

    def __get__(self, instance, owner):
        return partial(self.__call__, instance)
