1.pwc Sales project follows below project structure

script directory will contains the main scripts.
logs directory will contains the logs of execution.
data directory will json input data.
output directory will contains the output daily, summary files.
tests directory will contains the script for testing the script.
requirements.txt contains the packages required.

pwcSales
----- >scripts
	----- >__init__.py
	----- >pwcSalesSolution.py
----- >logs
	----- >pwcSalesExecutionyyyymmdd-hhmiss.log
----- >data
	----- >SalesData_2003.json
	----- >SalesData_2004.json
	----- >SalesData_2005.json 
----- >output
	----- >SaleValue.txt
	----- >SalesDataSummary.xlsx
	----- >DailySaleData_dd-mm-yyyy.gzip
----- >tests
	----- >__init__.py
	----- >test_pwcSalesSolution.py
	----- >testSalesData.json
	----- >testSalesDataLog.log
----- >requirements.txt


2. Analysis is mentioned in Analysis.docx

3. pwcSalesSolution.py in scripts directory contains the main script.

4. testSalesDataLog.log in tests directory contains the test script.