# encoding: utf-8
"""
公共装饰器
@author Yuriseus
@create 2016-5-11 14:59
"""
from functools import wraps
import time


def fun_cost_time(function):
    @wraps(function)
    def function_time(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        func_name = function.__name__
        print("cost_time %s: %s seconds" % (func_name, str(t1-t0)))
        return result
    return function_time


def do_hbase(function):
    @wraps(function)
    def oper(*args, **kwargs):
        result = function(*args, **kwargs)
        return result
    return oper
