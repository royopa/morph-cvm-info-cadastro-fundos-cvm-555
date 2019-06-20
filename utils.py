# -*- coding: utf-8 -*-
import datetime
import bizdays


def get_calendar():
    holidays = bizdays.load_holidays('ANBIMA.txt')
    return bizdays.Calendar(holidays, ['Sunday', 'Saturday'])


def isbizday(dt_referencia):
    cal = get_calendar()
    return cal.isbizday(dt_referencia)
