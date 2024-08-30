import datetime

__all__ = ['get_month_list']


def get_month_list(start_date, end_date):
    """

    :param start_date: datetime
    :param end_date: datetime
    :return: string list
    """
    if not (isinstance(start_date, datetime.datetime)&isinstance(end_date, datetime.datetime)):
        raise Exception("input start_date and end_date must be datetime type")

    delatYear = end_date.year - start_date.year
    months = end_date.month - start_date.month + delatYear * 12 + 1
    #  print(months)
    month_list = []
    for i in range(months):
        year = start_date.year + (start_date.month + i - 1) // 12
        month = (start_date.month + i - 1) % 12 + 1
        month_list.append("{}-{:0>2d}".format(year, month))

    return month_list
