import argparse
import pandas as pd
import glob
import csv
import os

def read_json(inputJsonDir,inputFilePrefix):
    jsonFiles=glob.glob(inputJsonDir + "\\" + inputFilePrefix + "*.json")
    jsonDF=pd.DataFrame()
    for file in jsonFiles:
        df = pd.read_json(file)
        jsonDF = pd.concat([jsonDF, df], axis=0)
    return jsonDF

def normalizeData(jsonDF):
    explodeDF=jsonDF.explode('attributes')
    normalDF=pd.json_normalize(explodeDF['attributes'])
    finaldf=pd.concat([explodeDF[['ORDERNUMBER','PRODUCTCODE']].reset_index(drop=True)
                      ,normalDF], axis=1)
    return finaldf

def TransformData(inputJsonDir,inputFilePrefix):
     salesDF=read_json(inputJsonDir,inputFilePrefix)
    print("salesDF shape is {} and columns are : {}"
                                 .format(str(salesDF.shape), str(salesDF.columns)))
    finalDF=normalizeData(salesDF)
    print("finalDF shape is {} and columns are : {}"
                                 .format(str(finalDF.shape), str(finalDF.columns)))

    # Add Year column
    finalDF['Year'] = pd.DatetimeIndex(finalDF['ORDERDATE']).year
    return finalDF

def writetoFile(txt):
    if os.path.exists(os.path.join(outputDir, outputFile)):
        with open(os.path.join(outputDir, outputFile), 'a') as f:
            f.write(txt)
            f.write('\n')
            f.write('\n')
            f.close()
    else:
        with open(os.path.join(outputDir, outputFile), 'w') as f:
            f.write(txt)
            f.write('\n')
            f.write('\n')
            f.close()

def totalSalesValue(df):
    #print(finalDF.STATUS.unique())
    txt = "Total Value of orders is : {}".format(df["SALES"].sum())
    writetoFile(txt)

def aggOutput(finalDF, grpBy, condCol, cond, aggCol, aggMethod, toBeColName, txt):
    df = finalDF[finalDF[condCol] == cond] \
        .groupby(grpBy) \
        .agg({aggCol: aggMethod}) \
        .reset_index() \
        .rename(columns={aggCol: toBeColName})

    writetoFile(txt)
    for index,row in df.iterrows():
        writetoFile(str(row[0]) + "   "  + str(row[1]))
    txt1=txt + " is : {}".format(df[toBeColName].sum())
    writetoFile(txt1)

def uniqueProdPerProdLine(df, grpBy, aggCol, aggMethod, toBeColName, txt):
    df=df.groupby(grpBy).agg({aggCol:aggMethod}) \
        .reset_index() \
        .rename(columns={aggCol:toBeColName})

    writetoFile(txt)
    for index,row in df.iterrows():
        writetoFile(str(row[0]) + "   "  + str(row[1]))

def salesTrend(df, grpBy, aggCol, aggMethod, toBeColName, txt):
    salesTrend=finalDF[((finalDF["PRODUCTLINE"]=="Classic Cars") & (finalDF["STATUS"]=="Shipped"))] \
                .groupby('Year') \
                .agg({"SALES":"sum",
                      "ORDERNUMBER":"count"}) \
                .reset_index() \
                .rename(columns={"SALES":"salesValue",
                                 "ORDERNUMBER":"noOfCars"})
    print(salesTrend)




def discountTable():
    dict = [{"minQty": 0, "maxQty": 30, "discPer": 0},
            {"minQty": 30, "maxQty": 60, "discPer": 2.5},
            {"minQty": 60, "maxQty": 80, "discPer": 4},
            {"minQty": 80, "maxQty": 100, "discPer": 6},
            {"minQty": 100, "maxQty": 99999999999, "discPer": 10}]
    df=pd.DataFrame.from_dict(dict)
    return df



if __name__=="__main__":
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_column',10)
    pd.set_option('display.max_colwidth', None)

    parser = argparse.ArgumentParser()
    parser.add_argument('--inputJsonDir',
                        default=r"C:\Users\n421492\PycharmProjects\Projects\pwcSales\data")
    parser.add_argument('--inputFilePrefix',
                        default="SalesData")
    parser.add_argument('--outputDir',
                        default=r"C:\Users\n421492\PycharmProjects\Projects\pwcSales\output")
    args=vars(parser.parse_args())

    finalData = TransformData(args['inputJsonDir'],
                              args['inputFilePrefix'])

    outputDir=args['outputDir']
    outputFile = "SalesData.txt"

    # find total value for orders
    totalSalesValue(finalData)

    # find total value for Cancelled orders
    aggOutput(finalData, 'Year', 'STATUS', 'Cancelled', 'SALES', 'sum', 'CancelSaleValue','Value of cancelled orders is')

    # find total value for on-Hold orders
    aggOutput(finalData, 'Year', 'STATUS', 'On Hold', 'SALES', 'sum', 'OnHoldSaleValue', 'Value of on-hold orders')

    # Products per productLine(df, grpBy, aggCol, aggMethod, toBeColName, txt)
    uniqueProdPerProdLine(finalData, 'PRODUCTLINE', 'PRODUCTCODE', 'nunique', 'noOfProducts', 'Products per productLine')

    # #Sales Trend of "Classic Cars" which were ""Shipped"
    # salesTrend=finalDF[((finalDF["PRODUCTLINE"]=="Classic Cars") & (finalDF["STATUS"]=="Shipped"))] \
    #             .groupby('Year') \
    #             .agg({"SALES":"sum",
    #                   "ORDERNUMBER":"count"}) \
    #             .reset_index() \
    #             .rename(columns={"SALES":"salesValue",
    #                              "ORDERNUMBER":"noOfCars"})
    # print(salesTrend)
    #
    #
    #
    # # Discount Per Quantity Ordered
    # discTab=discountTable()
    # print(discTab)
    # KeepProductLine=["Vintage Cars", "Classic Cars", "Trucks and Buses", "Motorcycles"]
    # discDF=finalDF[finalDF["PRODUCTLINE"].isin(KeepProductLine)] \
    #         [["ORDERNUMBER","PRODUCTCODE","QUANTITYORDERED","PRICEEACH","STATUS","PRODUCTLINE","MSRP"]]
    # #print(discDF.head())
    # discDF=discDF.head(30)
    # newDF=pd.DataFrame()
    # for index, row in discDF.iterrows():
    #     for index,discRow in discTab.iterrows():
    #         if ((row["QUANTITYORDERED"] >= discRow["minQty"])
    #             & (row["QUANTITYORDERED"] < discRow["maxQty"])):
    #             #minQty = discRow["minQty"]
    #             #maxQty = discRow["maxQty"]
    #             discPer=discRow["discPer"]
    #             print(row["QUANTITYORDERED"],discPer)
    #     #row['minQty'] = minQty
    #     #row['maxQty'] = maxQty
    #     row['discPer'] = discPer
    #     row['NetMSRPVal'] = row['MSRP'] - (row['MSRP']*discPer/100)
    #     #newDF = newDF.append(row, ignore_index=True)
    #     newDF = pd.concat([newDF, row.to_frame().T])
    #
    # print(newDF.head(30))