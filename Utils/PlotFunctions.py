import os
import matplotlib.pyplot as plt
import numpy as np
from reportlab.platypus import Image
from reportlab.lib.units import inch
import time


def plot_group_bar2(input_list, title_name, pic_name, report_address):
    plt.figure(figsize=(6, 2), dpi=300)
    plt.bar(range(input_list.__len__()), input_list)
    plt.xticks(np.arange(input_list.__len__()), list(np.arange(input_list.__len__())), fontsize=3)
    plt.yticks(fontsize=5)
    plt.title(title_name, fontsize=5)
    file_name = os.path.join(report_address, pic_name)
    plt.savefig(file_name, format='png')
    im = Image(file_name, 9 * inch, 3 * inch)
    return im


def plot_series(series_intput, x_label, y_label, pic_title, pic_name, report_address, legend_location='best'):
    time_stamp_list = series_intput[0].index
    data_list = []
    for i in range(series_intput.__len__()):
        temp_data = series_intput[i].values.tolist()
        data_list.append(temp_data)
    index = []
    index_number = []
    x_number = []
    for i, time_stamp in enumerate(time_stamp_list):
        x_number.append(i)
        if i % int(time_stamp_list.__len__() / 6) == 0:
            index_number.append(i)
            timearray = time.localtime(time_stamp)
            index.append(time.strftime('%Y%m%d', timearray))
    x_number = np.array(x_number)
    plt.figure(figsize=(6, 2), dpi=300)
    for i in range(series_intput.__len__()):
        plt.plot(x_number, np.array(data_list[i]), linewidth=0.3, label=x_label[i])
    plt.xticks(index_number, index, fontsize=5, rotation=0)
    plt.ylabel(y_label, fontsize=5)
    plt.title(pic_title, fontsize=5)
    plt.yticks(fontsize=5)
    plt.legend(loc=legend_location, fontsize=4)
    file_name = os.path.join(report_address, pic_name)
    plt.savefig(file_name, format='png')
    im = Image(file_name, 9 * inch, 3 * inch)
    return im


def plot_one_series(series_intput, x_label, y_label, pic_title, pic_name, report_address):
    time_stamp_list = series_intput.index
    data = series_intput.values.tolist()
    index = []
    index_number = []
    x_number = []
    for i, time_stamp in enumerate(time_stamp_list):
        x_number.append(i)
        if i % int(time_stamp_list.__len__() / 6) == 0:
            index_number.append(i)
            timearray = time.localtime(time_stamp)
            index.append(time.strftime('%Y%m%d', timearray))
    x_number = np.array(x_number)
    plt.figure(figsize=(6, 2), dpi=300)
    plt.plot(x_number, np.array(data), color='red', linewidth=0.3, label=x_label)
    plt.xticks(index_number, index, fontsize=5, rotation=0)
    plt.ylabel(y_label, fontsize=5)
    plt.title(pic_title, fontsize=5)
    plt.yticks(fontsize=5)
    plt.legend(loc='best', fontsize=5)
    file_name = os.path.join(report_address, pic_name)
    plt.savefig(file_name, format='png')
    im = Image(file_name, 9 * inch, 3 * inch)
    return im
