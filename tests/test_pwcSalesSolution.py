import pytest
from scripts.pwcSalesSolution import read_json, normalizeData, setupData
import os

@pytest.fixture
def cwd():
    return os.getcwd()

@pytest.fixture
def jsonDF(cwd):
    return read_json(cwd, "testSalesData", cwd, "testSalesDataLog.log")

def test_read_json(cwd, jsonDF):
    assert jsonDF.shape[0] == 4     #No of rows
    assert jsonDF.shape[1] == 3     #No of columns

def test_normalizeData(cwd, jsonDF):
    df=normalizeData(jsonDF, cwd, "testSalesDataLog.log")
    assert df.shape[0] == 4     #No of rows
    assert df.shape[1] == 9     #No of columns

def test_setupData(cwd):
    df = setupData(cwd, "testSalesData", cwd, "testSalesDataLog.log")
    assert df.shape[0] == 4     #No of rows
    assert df.shape[1] == 12     #No of columns
    assert df.columns[9] == 'Year'  # Checks additional columns at 9th index
    assert df.columns[10]=='Month'  # Checks additional columns at 10th index
    assert df.columns[11] == 'Day'  # Checks additional columns at 11th index