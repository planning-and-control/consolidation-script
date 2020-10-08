import pandas as pd
import os
import xlrd
import datetime
from dateutil.relativedelta import relativedelta
from variables import *

def delete_and_save(df, output_path, filename):
    final_path = os.path.join(output_path, filename)
    if os.path.exists(final_path):
        os.remove(final_path)
        df.to_csv(final_path)
    else:
        df.to_csv(final_path)

def transform_adaptive_out(df, date, D_AU):
    
    #transform excel-like dates to regular format dates
    # df.loc[:,"dataPeriod"] = df.dataPeriod.map(lambda x: excel_to_datetime(x))
    # df["dataPeriod_datetime"] = pd.to_datetime(df["dataPeriod"], format="%d/%m/%Y")

    #flter dataframe
    index_drop = df[df.dataPeriod < date].index
    df = df.drop(index_drop)

    #add SC column
    df["D_SC"] = "FC20 (3+9)"

    #rename LC_Amount column
    df = df.rename(columns={"Amount": "LC_Amount"})

    #drop rows where LevelName is Blue Canyon I Company
    index_drop = df[df.LevelName == "Blue Canyon I Company"].index
    df = df.drop(index_drop)
    
    #add D_AU column
    df["D_AU"] = D_AU

    #add LevelName column
    df["Period_Level"] = df["LevelName"] + "_" + df["dataPeriod"].dt.strftime("%Y_%m")

    #add PeriodPartner
    df["Period_Partner"] = df["Partner"] + "_" + df["dataPeriod"].dt.strftime("%Y_%m")

    #remove selected columns
    col_drop = ["CompanyCode", "Country_Load", "Level Type", "platformAccount", "BSSourceAccount", "Currency", "Rolls up to", "Park", "IsCP", "IsLinkCalc", "OM_Service"]
    df.drop(col_drop, axis=1, inplace=True)

    return df

def create_consoflag_table(df_adaptive_master_levels, df_datareport_0):
    
    #promoting columns
    headers = df_adaptive_master_levels.iloc[1]
    df_adaptive_master_levels  = pd.DataFrame(df_adaptive_master_levels.values[2:], columns=headers)

    #add consoflag column
    df_adaptive_master_levels["ConsoFlag"] = 0

    #expanding df with dates
    from variables import start_date, end_date
    dates = []
    while (start_date <= end_date):
        dates.append(start_date.strftime("%d/%m/%Y"))
        start_date = start_date + relativedelta(months=1)

    df_adaptive_master_levels = df_adaptive_master_levels[["ConsoFlag", "Name"]]
    df_adaptive_master_levels = pd.concat([df_adaptive_master_levels, pd.DataFrame(columns=dates)])
    df_adaptive_master_levels = df_adaptive_master_levels.melt(id_vars=["Name", "ConsoFlag"])
    df_adaptive_master_levels = df_adaptive_master_levels.rename(columns={"variable": "Period", "Name": "Level"})
    df_adaptive_master_levels.drop(["value"], axis=1, inplace=True)
    
    #selecting columns for datareport 0
    df_datareport_0 = df_datareport_0[["Period", "Level", "Amount"]]
    df_datareport_0 = df_datareport_0.rename(columns={"Period": "Period_datetime"})
    df_datareport_0["Period"] = df_datareport_0["Period_datetime"].dt.strftime("%d/%m/%Y")
    df_datareport_0.drop(["Period_datetime"], axis=1, inplace=True)

    #rename Amount column
    df_datareport_0 = df_datareport_0.rename(columns={"Amount": "ConsoFlag"})

    df_appended = pd.concat([df_adaptive_master_levels, df_datareport_0])
    df_appended = df_appended.rename(columns={"ConsoFlag": "ConsoFlagFinal"})
    df_appended.groupby(["Period", "Level"], as_index=False).max()
    
    #add PeriodLevel
    df_appended["dataPeriod_datetime"] = pd.to_datetime(df_appended["Period"], format="%d/%m/%Y")
    df_appended["Period_Level"] = df_appended["Level"] + "_" + df_appended["dataPeriod_datetime"].dt.strftime("%Y_%m")
    df_appended.drop(["dataPeriod_datetime"], axis=1, inplace=True)
    return df_appended

def consoPartnerFlag(df_datareport_0):
    #drop rows where LevelName is Blue Canyon I Company
    index_drop = df_datareport_0[df_datareport_0["Level"] == "Blue Canyon I Company"].index
    df_datareport_0 = df_datareport_0.drop(index_drop)
    df_datareport_0 = df_datareport_0[["Period", "Company Code", "Amount"]]

    #rename Amount column
    df_datareport_0 = df_datareport_0.rename(columns={"Amount": "ConsoFlag", "Company Code": "Partner"})

    #filter where partner is not empty
    index_drop=df_datareport_0[df_datareport_0["Partner"].isnull()].index
    df_datareport_0 = df_datareport_0.drop(index_drop)

    #add PeriodLevel
    df_datareport_0["dataPeriod_datetime"] = pd.to_datetime(df_datareport_0["Period"], format="%d/%m/%Y")
    df_datareport_0["Period_Partner"] = df_datareport_0["Partner"] + "_" + df_datareport_0["dataPeriod_datetime"].dt.strftime("%Y_%m")
    df_datareport_0.drop(["dataPeriod_datetime"], axis=1, inplace=True)

    return df_datareport_0

def xlsx_to_csv(input_path, output_path):
    files_input = os.listdir(input_path)
    files_output = os.listdir(output_path)
    for file in files_input:
        file_name = str(file[:-4])
        if file_name+"csv" not in files_output:
            df = pd.read_excel(os.path.join(input_path, file))
            file_name = file_name+"csv"
            df.to_csv(os.path.join(output_path, file_name))
            print(str(file_name)+" created")

def excel_to_datetime(excel_date):
    a = xlrd.xldate.xldate_as_tuple(excel_date, 0)
    return datetime.datetime(*a).strftime("%d/%m/%Y")

def partnerEquityOut(df, df_flags, D_AU):
    merging_columns = ["ConsoFlag", "Period_Partner"]

    df_flags_partner = df.merge(df_flags[merging_columns], on="Period_Partner", how="left")
    
    df_filtered = df_flags_partner[df_flags_partner.ConsoFlag == 1]
    
    df_filtered_a = df_filtered.copy()
    df_filtered_b = df_filtered.copy()

    df_filtered_a.loc[:,"LC_Amount"] = df_filtered_a["LC_Amount"].multiply(-1)
    
    #change account code
    df_filtered_b.loc[df_filtered_b.FlowAccount.isnull(), "AccountCode"] = "R680L"
    df_filtered_b.loc[(df_filtered_b.FlowAccount.isnull() == False), "AccountCode"] = "P606E"
    
    #change codeAcc
    df_filtered_b.loc[df_filtered_b.FlowAccount.isnull(), "codeAcc"] = "R680L"
    df_filtered_b.loc[(df_filtered_b.FlowAccount.isnull() == False), "codeAcc"] = "P606E"   
    
    #change AccountName
    df_filtered_b.loc[df_filtered_b.FlowAccount.isnull(), "AccountName"] = "R680L"
    df_filtered_b.loc[(df_filtered_b.FlowAccount.isnull() == False), "AccountName"] = "P606E"   
    
    #change Partner
    df_filtered_b.loc[:, "Partner"] = "#"    
    
    #concat dataframes
    df_final = pd.concat([df_filtered_a, df_filtered_b])
    
    #change D_AU
    df_final.loc[:, "D_AU"] = D_AU
    
    #remove columns
    col_drop = ["ConsoFlag"]
    df_final.drop(col_drop, axis=1, inplace=True)
    
    return df_final

def equity_out(df, df_flags, D_AU):
    merging_columns = ["ConsoFlagFinal", "Period_Level"]
    df_flags = df.merge(df_flags[merging_columns], on="Period_Level", how="left")
    df_1LIA05 = df_flags[df_flags.ConsoFlagFinal == 0]
    df_1LIA05.loc[:,"LC_Amount"] = df_1LIA05["LC_Amount"].multiply(-1)
    df_1LIA05.loc[:,"D_AU"] = D_AU
    col_drop = ["ConsoFlagFinal"]
    df_1LIA05.drop(col_drop, axis=1, inplace=True)
    return df_1LIA05

def data_out(df, df_flags):
    merging_columns = ["ConsoFlagFinal", "Period_Level"]
    df_0LIA01_1LIA05 = df.merge(df_flags[merging_columns], on="Period_Level", how="left")
    df_0LIA01_1LIA05 = df_0LIA01_1LIA05[df_0LIA01_1LIA05.ConsoFlagFinal == 1]
    col_drop = ["ConsoFlagFinal"]
    df_0LIA01_1LIA05.drop(col_drop, axis=1, inplace=True)
    return df_0LIA01_1LIA05

def read_path(input_all_paths, denomination):
    df = pd.read_excel(input_all_paths, sheet_name="inputs")
    return df[df.denomination==denomination].path.iloc[0]

def check_subconso(string):
    subconso_flag = ["2ELI10", "1IFRS002"]
    if string in subconso_flag:
        return "Subconsolidated"
    else:
        return "Consolidated"

# def pipeline_conso(path_adaptive_out, path_adaptive_mje, path_flags, path_flags):

#     #creation of dataframes

#     df_adaptive = pd.read_csv(r'c:\Users\E353952\Desktop\code-projects\consolidation\input\csv-format\Adaptive_out.csv', delimiter=";", encoding="cp1252", parse_dates=["dataPeriod"])