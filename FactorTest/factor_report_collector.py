import pandas as pd
import platform
import json
import os


if platform.system() == "Windows":  # 云桌面环境运行是Windows
    report_dir = r'S:\Apollo\FactorTestReport'
else:
    report_dir = "/app/data/006566/Apollo/factor_report"
report_file = os.listdir(report_dir)
pdx = pd.DataFrame([])
for report_json in report_file:
    if report_json[-4:] == 'json':
        with open(report_dir+'/'+report_json) as f:
            report_content = json.load(f)
            for i_key in report_content:
                if isinstance(report_content[i_key], list):
                    if report_content[i_key].__len__() > 1:
                        key_values = None
                        for j, element in enumerate(report_content[i_key]):
                            if j > 0:
                                key_values = key_values + ', ' + str(element)
                            else:
                                key_values = str(element)
                        report_content[i_key] = key_values
                    elif report_content[i_key].__len__() == 0:
                        report_content[i_key] = " "
            if pdx.empty:
                pdx = pd.DataFrame(report_content, index=[0])
                pdx = pdx.set_index(['Factor_name'])
            else:
                temp = pd.DataFrame(report_content, index=[0])
                temp = temp.set_index(['Factor_name'])
                pdx = pdx.append(temp)
pdx.to_excel("factor_report" + ".xlsx")
