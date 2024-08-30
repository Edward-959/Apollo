# -*- coding: utf-8 -*-
"""
Created on 2018/11/9
@author: 011669
"""


from scipy.optimize import leastsq


def func(p, x):

    k, b = p
    return k * x + b


def error(p, x, y):
    # print(s)
    return func(p, x) - y
    # x、y都是列表，故返回值也是个列表


def lsfit(errorfunc, xi, yi):
    """
    :param errorfunc: error 把error函数中除了p以外的参数打包到args中
    :param xi: x s = "Test the number of iteration"
    :param yi: y
    :return: k, b
    """
    # p0 = [1, 1]
    para = leastsq(errorfunc, [0.01, 1], args=(xi, yi))
    k, b = para[0]
    # print("k=", k, '\n', "b=", b)
    return k, b

