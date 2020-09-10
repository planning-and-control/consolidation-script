import pandas as pd
from functions import *
from variables import *
import os

df_adaptive_0 = pd.read_csv(path_adaptive_out, delimiter=";", encoding="cp1252", parse_dates=["dataPeriod"])
df_mje = pd.read_csv(path_adaptive_mje, delimiter=";", encoding="cp1252", parse_dates=["dataPeriod"])


df_flags = pd.read_excel(path_flags, sheet_name="Sheet2")
df_partner_flag = pd.read_excel(path_flags, sheet_name="Sheet1")

df_0LIA01 = transform_adaptive_out(df_adaptive_0, "2020-04-01", "0LIA01")
df_0LIA01.to_csv(os.path.join(output_path, "BP2025_0LIA01.csv"))

df_1LIA05 = equity_out(df_0LIA01, df_flags, "1LIA05")
df_1LIA05.to_csv(os.path.join(output_path, "BP2025_1LIA05.csv"))

df_0LIA01_1LIA05 = data_out(df_0LIA01, df_flags)
df_0LIA01_1LIA05.to_csv(os.path.join(output_path, "BP2025_0LIA01_1LIA05.csv"))

df_2ELI10 = partnerEquityOut(df_0LIA01_1LIA05, df_partner_flag, "2ELI10")
df_2ELI10.to_csv(os.path.join(output_path, "BP2025_2ELI10.csv"))


df_1IFRS000 = transform_adaptive_out(df_mje, "2020-04-01", "1IFRS000")
df_1IFRS000.to_csv(os.path.join(output_path, "BP2025_1IFRS000.csv"))

df_1IFRS001 = equity_out(df_1IFRS000, df_flags, "1IFRS001")
df_1IFRS001.to_csv(os.path.join(output_path, "BP2025_1IFRS001.csv"))

df_1IFRS002 = partnerEquityOut(df_1IFRS000, df_partner_flag, "1IFRS002")
df_1IFRS002.to_csv(os.path.join(output_path, "BP2025_1IFRS002.csv"))

df_final = pd.concat([df_0LIA01_1LIA05, df_2ELI10, df_1IFRS000, df_1IFRS001, df_1IFRS002])

df_final.reset_index(drop=True, inplace=True)

index_drop = df_final[df_final.FlowAccount.isnull()].index
df_final = df_final.drop(index_drop)

index_drop = df_final[df_final.dataPeriod.dt.year == 2025].index

df_final = df_final.drop(index_drop)



df_final.to_csv(os.path.join(output_path, "BP2025_F00_fromFxx_prev.csv"))

time_increase = pd.Timedelta(days=365)
df_final.dataPeriod += time_increase

df_final = df_final.astype({'dataPeriod': 'str', 'LevelName': "str", 'AccountCode': "str", 'CostCentre': "str", 'Partner': "str", 'codeAcc': "str", 'intercoAccount': "str", 'FlowAccount': "str",
                            'AccountName': "str",'D_SC': "str", 'D_AU': "str", 'Period_Level': "str", 'Period_Partner': "str", "LC_Amount": float})

df_final.loc[:,"FlowAccount"] = "F00"
df_final.loc[:,"intercoAccount"] = "C"
df_final.loc[:,"AccountCode"] = df_final.codeAcc+ "_F00_CH"
df_final.loc[:,"AccountName"] = df_final.codeAcc+ "_F00_CH"



grouping_cols = ['dataPeriod', 'LevelName', 'AccountCode', 'CostCentre', 'Partner',
       'codeAcc', 'intercoAccount', 'FlowAccount', 'AccountName',
       'D_SC', 'D_AU', 'Period_Level', 'Period_Partner']

df_final = df_final.groupby(grouping_cols, as_index=False).sum()
df_final.to_csv(os.path.join(output_path, "BP2025_F00_fromFxx.csv"))