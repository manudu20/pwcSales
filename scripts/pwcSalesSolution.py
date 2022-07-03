import argparse
import pandas as pd
import glob
import os
import logging
from datetime import datetime
import sys

#Function to setup configuration for logging
def logConfig(logdir, logfile):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True,
        handlers=[
            logging.FileHandler(os.path.join(logdir, logfile)),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    return logger

#Function to Read json files and output in single pandas dataframe
def read_json(inputJsonDir, inputFilePrefix, logDir, logFileName):
    logger=logConfig(logDir,logFileName)
    logger.info('function read_json started')

    # Join all json files in input directory into a list
    jsonFiles = glob.glob(inputJsonDir + "\\" + inputFilePrefix + "*.json")
    logger.info('Json files are {}'.format(jsonFiles))

    # Read all json files and write into a single pandas dataframe
    jsonDF = pd.DataFrame()
    for file in jsonFiles:
        try:
            df = pd.read_json(file)
        except FileNotFoundError as e:
            logger.error('File {} not found'.format(file) + str(e))
            sys.exit()
        except pd.errors.EmptyDataError as e:
            logger.error('File {} is empty found'.format(file) + str(e))
        except pd.errors.ParserError as e:
            logger.error('File {} has parsing error'.format(file) + str(e))
            sys.exit()
        except Exception as e:
            logger.error('File {} reading has error'.format(file) + str(e))
            sys.exit()
        finally:
            logger.info('Json file {} read successfully'.format(file))
            jsonDF = pd.concat([jsonDF, df], axis=0)
    logger.info('Json file {} read successfully'.format(file))
    logger.info('function read_json ended')
    return jsonDF

#Normalize the dataframe due to "listed dictionary" in a attribute
def normalizeData(jsonDF, logDir, logFileName):
    logger = logConfig(logDir, logFileName)
    logger.info('function normalizeData started')

    #convert columns to row from  attributes list
    try:
        explodeDF = jsonDF.explode('attributes')
    except Exception as e:
        logger.error('DataFrame explode has errors'+ str(e))
    finally:
        logger.info('DataFrame exploding done')

    # transform dictionary attribute keys into columns
    try:
        normalizeDF = pd.json_normalize(explodeDF['attributes'])
    except Exception as e:
        logger.error('json df normalization has errors'+ str(e))
    finally:
        logger.info('json df normalization done')

    # Join both dataframes for final Output in a dataframe
    try:
        finalDf = pd.concat([explodeDF[['ORDERNUMBER', 'PRODUCTCODE']].reset_index(drop=True),
                         normalizeDF], axis=1)
    except Exception as e:
        logger.error('joining explode and normalize dataframe has errors'+ str(e))
    finally:
        logger.info('joining explode and normalize dataframe done')

    logger.info('function normalizeData ended')
    return finalDf

# Entry point to program to setup data from json files, calling other functions to transform
# json files into pandas dataframe
def setupData(inputJsonDir, inputFilePrefix, logDir, logFileName):
    logger = logConfig(logDir, logFileName)
    logger.info('function setupData started')

    try:
        # read json files and output in a pandas dataframe
        salesDF = read_json(inputJsonDir, inputFilePrefix, logDir, logFileName)
        logger.info("salesDF shape is {} and columns are : {}".format(str(salesDF.shape), str(salesDF.columns)))
        # transform json dictionary data into a final readable dataframe
        finalDF = normalizeData(salesDF, logDir, logFileName)
        logger.info("finalDF shape is {} and columns are : {}".format(str(finalDF.shape), str(finalDF.columns)))

        # Add Year, Month and Day column to use further in calculation
        finalDF['Year'] = pd.DatetimeIndex(finalDF['ORDERDATE']).year.astype(str)
        finalDF['Month']=pd.DatetimeIndex(finalDF['ORDERDATE']).month.astype(str)
        finalDF['Day'] = pd.DatetimeIndex(finalDF['ORDERDATE']).day.astype(str)
        logger.info('function setupData ended')
    except Exception as e:
        logger.error('function setupData has errors'+ str(e))
        raise Exception
    finally:
        logger.info('function setupData ended')

    return finalDF

#Function to write daily file for each day sale data
def writedailyFile(df, outDir):
    logger.info('function writedailyFile started')

    try:
        df['DMY'] = df['Day'] + "-" + df['Month'] + "-" + df['Year']
        dates = list(df.DMY.unique())
        logger.info('No of files generated: {}'.format(str(len(dates))))
        for date in dates:
             dateDF=df[df['DMY']==date][['ORDERNUMBER', 'PRODUCTCODE', 'QUANTITYORDERED', 'PRICEEACH', 'SALES', 'ORDERDATE', 'STATUS', 'PRODUCTLINE', 'MSRP']]
             dateDF.to_parquet(os.path.join(outDir, ("DailySaleData_"+date+".gzip")), engine='auto', compression='gzip')
    except Exception as e:
        logger.error('function writedailyFile has errors'+ str(e))
        raise Exception
    finally:
        logger.info('function writedailyFile ended')


#create text file to write total value , cancelled and on hold orders of Sales
def findAndWriteSaleValue(df, condCol, cond, aggCol, outDir, mode):
    logger.info('function findAndWriteSaleValue started')

    if not isinstance(aggCol, str):
        err='Argument {} is of type {}, it should be string'.format(aggCol, type(aggCol))
        logger.error(err)
        raise TypeError(err)
    if not isinstance(outDir, str):
        err='Argument {} is of type {}, it should be string'.format(outDir, type(outDir))
        logger.error(err)
        raise TypeError(err)

    if condCol==None:
        Val = df[aggCol].sum()
        txt = "Total Value of orders is : {}".format(Val)
        logger.info('condCol is None')
    else:
        Val= df[df[condCol] == cond][aggCol].sum()
        txt = "Total Value of {} orders is : {}".format(cond, Val)
        logger.info('condCol is {}'.format(condCol))

    try:
        with open(os.path.join(outDir, 'SaleValue.txt'), mode) as f:
            f.write(txt)
            f.write('\n')
            f.write('\n')
            f.close()
    except IOError as e:
        logger.error('File {} writing has errors'.format('SaleValue.txt') + str(e))
        raise IOError
    except Exception as e:
        logger.error('function findAndWriteSaleValue has errors' + str(e))
        raise Exception
    finally:
        logger.info('function findAndWriteSaleValue ended')

#To find Yearly Sales value of Cancelled/on-hold orders
#(finalData, 'Year', 'STATUS', 'Cancelled', 'SALES', 'sum', 'CancelSaleValue',
#              'Value of cancelled orders is', outDir,  'YearlyCancelOrders.csv')
def aggWriteStatusData(finalDF, grpBy, condCol, cond, aggCol, aggMethod, toBeColName, writer, sheetName):
    logger.info('function aggWriteStatusData started')

    try:
        df = finalDF[finalDF[condCol] == cond] \
            .groupby(grpBy) \
            .agg({aggCol: aggMethod}) \
            .reset_index() \
            .rename(columns={aggCol: toBeColName})
        df.to_excel(writer, sheet_name=sheetName, index=False)
        writer.save()
    except IOError as e:
        logger.error('aggWriteStatusData has error while writing file' + str(e))
        raise IOError
    except Exception as e:
        logger.error('function aggWriteStatusData has other errors' + str(e))
        raise Exception
    finally:
        logger.info('function aggWriteStatusData ended')

def AggWriteData(df, grpBy, aggCol, aggMethod, toBeColName, writer, sheetName):
    logger.info('function AggWriteData started')

    try:
        df = df.groupby(grpBy).agg({aggCol: aggMethod}) \
            .reset_index() \
            .rename(columns={aggCol: toBeColName})

        df.to_excel(writer, sheet_name=sheetName, index=False)
        writer.save()
    except IOError as e:
        logger.error('AggWriteData has error while writing file' + str(e))
        raise IOError
    except Exception as e:
        logger.error('function AggWriteData has other errors' + str(e))
        raise Exception
    finally:
        logger.info('function AggWriteData ended')

def findAndWriteSalesTrend(df, condsCol1, inCols1, condsCol2, inCols2 , grpByCols, writer, sheetName):
    logger.info('function findAndWriteSalesTrend started')

    try:
        df = df[((df[condsCol1].isin(inCols1)) & (df[condsCol2].isin(inCols2)))] \
            .groupby(grpByCols) \
            .agg({"SALES": "sum","ORDERNUMBER": "count"}) \
            .reset_index() \
            .rename(columns={'SALES': 'YearlySales','ORDERNUMBER': 'YearlyQuantity'})
        df.to_excel(writer, sheet_name=sheetName, index=False)
        writer.save()
    except IOError as e:
        logger.error('findAndWriteSalesTrend has error while writing file' + str(e))
        raise IOError
    except Exception as e:
        logger.error('function findAndWriteSalesTrend has other errors' + str(e))
        raise Exception
    finally:
        logger.info('function findAndWriteSalesTrend ended')


# Calculate Discount Per Quantity Ordered
def CalculateAndWriteDiscount(finalDF, writer, sheetName):
    logger.info('function CalculateAndWriteDiscount started')

    def discountTable():
        dict = [{"minQty": 0, "maxQty": 30, "discPer": 0},
                {"minQty": 30, "maxQty": 60, "discPer": 2.5},
                {"minQty": 60, "maxQty": 80, "discPer": 4},
                {"minQty": 80, "maxQty": 100, "discPer": 6},
                {"minQty": 100, "maxQty": 99999999999, "discPer": 10}]
        discDF = pd.DataFrame.from_dict(dict)

        try:
            discDF.to_excel(writer, sheet_name='Discount Table', index=False)
            writer.save()
        except IOError as e:
            logger.error('discountTable has error while writing file' + str(e))
            raise IOError
        except Exception as e:
            logger.error('function discountTable has other errors' + str(e))
            raise Exception
        finally:
            logger.info('function discountTable ended')
        return discDF

    discTab=discountTable()
    logger.info('discount table initialized')
    KeepProductLine=["Vintage Cars", "Classic Cars", "Trucks and Buses", "Motorcycles"]
    logger.info('Allowed product Lines are {}'.format(KeepProductLine))
    filteredDF=finalDF[finalDF["PRODUCTLINE"].isin(KeepProductLine)] \
             [["ORDERNUMBER","PRODUCTCODE","QUANTITYORDERED","PRICEEACH","STATUS","PRODUCTLINE","MSRP"]]
    logger.info('Sales Data filtered with product Lines'.format(KeepProductLine))
    discDF=pd.DataFrame()
    for index, row in filteredDF.iterrows():
        for index,discRow in discTab.iterrows():
            if ((row["QUANTITYORDERED"] >= discRow["minQty"])
                        & (row["QUANTITYORDERED"] < discRow["maxQty"])):
                discPer=discRow["discPer"]

        row['discPer'] = discPer
        row['NetMSRPVal'] = row['MSRP'] - (row['MSRP']*discPer/100)
        discDF = pd.concat([discDF, row.to_frame().T])

    logger.info('Discount calculated for sales data')

    try:
        discDF.to_excel(writer, sheet_name=sheetName, index=False)
        writer.save()
    except IOError as e:
        logger.error('CalculateAndWriteDiscount has error while writing file' + str(e))
        raise IOError
    except Exception as e:
        logger.error('function CalculateAndWriteDiscount has other errors' + str(e))
        raise Exception
    finally:
        logger.info('function CalculateAndWriteDiscount ended')

def writeOutput(finalData, outDir):
    logger.info('function writeOutput started')

    #Write daily File with daily partitions
    writedailyFile(finalData, outDir)

    # find total value of orders
    findAndWriteSaleValue(finalData, None, None , 'SALES', outDir, 'w')
    # find total value for cancelled orders
    findAndWriteSaleValue(finalData, 'STATUS', 'Cancelled', 'SALES', outDir, 'a')
    # find total value for on-hold orders
    findAndWriteSaleValue(finalData, 'STATUS', 'On Hold', 'SALES', outDir, 'a')

    writer = pd.ExcelWriter(os.path.join(outDir, 'SalesDataSummary.xlsx'), engine='openpyxl')

    # Yearly Sale Value(df, grpBy, aggCol, aggMethod, toBeColName, txt)
    AggWriteData(finalData, 'Year', 'SALES', 'sum',
                     'SaleValue', writer, 'YearlySaleValue')

    # Yearly status Sale Value(df, grpBy, aggCol, aggMethod, toBeColName, txt)
    AggWriteData(finalData, ['Year','STATUS'], 'SALES', 'sum',
                     'SaleValue', writer, 'YearlyStatusSaleValue')

    # find total value for Cancelled orders
    aggWriteStatusData(finalData, 'Year', 'STATUS', 'Cancelled', 'SALES', 'sum',
                       'CancelSaleValue', writer, 'YearlyCancelledOrders')

    # find total value for on-Hold orders
    aggWriteStatusData(finalData, 'Year', 'STATUS', 'On Hold', 'SALES', 'sum',
                       'OnHoldSaleValue', writer, 'YearlyOnHoldOrders')

    # Products per productLine(df, grpBy, aggCol, aggMethod, toBeColName, txt)
    AggWriteData(finalData, 'PRODUCTLINE', 'PRODUCTCODE', 'nunique',
                     'noOfProducts', writer, 'ProductPerProductLine')

    # YearlySalesTrend
    findAndWriteSalesTrend(finalData, "PRODUCTLINE", ['Classic Cars'],
                          "STATUS", ['Shipped'],
                          ['Year'],
                          writer,
                          "YearlyTrendForShipClassCars")

    #Discount on MRSP Per Quantity Ordered as per disc table
    CalculateAndWriteDiscount(finalData, writer, 'DiscountedRates')

    logger.info('function writeOutput ended')

if __name__ == "__main__":
    #Set Pandas frame to show all columns
    pd.set_option('display.width', 3000)
    pd.set_option('display.max_column', 30)
    pd.set_option('display.max_colwidth', None)

    #Define Arguments for input, output and Logs
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputJsonDir',
                        default=os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)),'data'))
    parser.add_argument('--inputFilePrefix',
                        default="SalesData")
    parser.add_argument('--outputDir',
                        default=os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)),'output'))
    parser.add_argument('--logDir',
                        default=os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)),'logs'))
    parser.add_argument('--logFileName',
                        default="pwcSalesExecution" + str(datetime.now().strftime('%Y%m%d-%H%M%S')) + ".log")

    args = vars(parser.parse_args())

    logDir=args['logDir']
    logfileName=args['logFileName']
    logger=logConfig(logDir, logfileName)

    logger.info('pwcSalesSolution Started')

    # Transform Json file data into pandas DataFrame
    finalData = setupData(args['inputJsonDir'],
                          args['inputFilePrefix'],
                          logDir,
                          logfileName)

    # Generate required output and write into excel file
    writeOutput(finalData, args['outputDir'])

    logger.info('pwcSalesSolution Completed')