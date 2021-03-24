# Introduction
Nasa Web Access Log Analyzer Application

## Responsible For
- Returns Top N visitor (reads N from the config file)
- Returns Top N urls (reads N from the config file)

### Class Structure
Input Download URL - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz	
1. src/main/scala
	* *gov.nasa.loganalyzer.InvokeNasaProcessor.scala* - This is main entry class. SparkSession object is build here.
	* *gov.nasa.loganalyzer.NasaStats.scala* - The spark Dataset processing logic is present here.
2. src/main/resources
	* *accessloganalyzer.conf* - all configurations are maintained in a  config file.
3. src/test/scala
	* *gov.nasa.test.loganalyzer.InvokeNasaProcessorTest.scala* - This is the Unit test class for InvokeNasaProcessor.
4. *ScalaSyleGuide.xml* - This is the formatter configuration file used to format the code.

### Configurations
Configuration file name- *accessloganalyzer.conf*

Properties set in configuration file-
- **numberOfResults**- This property is to set the value of N in the topNVisitors and topNUrls. *REQUIRED INT value*
- **writeCurruptLogEnteries**- Some of the log lines are corrupt. If writeCurruptLogEnteries is set to TRUE, then corrupt lines will be writen to filesystem, else not. *BOOLEAN value*
- **filterResponseCodes**- When evaluating topNUrls, this property is used to filter the log lines with undesired response code. Eg- when set to value ["304","400"], all log lines with response code 304 and 400 will be filtered. *LIST value*
- **downloadFileLoc**- This is the ftp location from where the input gz file will be downloaded. If set to blank, then file-download will not happen, assuming that the file is already on local filesystem. *STRING value*
- **gzFSLocation**- This is the local filesystem path where gz file is downloaded. If the property "downloadFileLoc" is not set, then the application assumes that the gz file is present at this location. *REQUIRED STRING value*
	
 The configuration properties are mandatory if not set, the application exits with code 0.


### Steps to compile
1. Go to the project root directory, where build.sbt is present
2. Run cmd- `sbt clean assembly`. By default this will not run test.Refer the below section to see how to run tests.
3. The jar is generated in the target directory. Check jar full path in the console-log.


### Steps to run unit test
1. Go to the project root directory, where build.sbt is present
2. Run cmd- `sbt test`.
3. The test report is generated in the console-log.

### Steps to run the application on the local machine
#### System Setup
mkdir <app_base_path>/conf

mkdir <app_base_path>/input

mkdir <app_base_path>/input/jar

mkdir <app_base_path>/output
	
	
Copy accessloganalyzer.conf to <app_base_path>/conf

Copy jar to <app_base_path>/input/jar

Update accessloganalyzer.conf environment specific properties.

Run application

#### Command to execute	
```
spark-submit \
--class gov.nasa.loganalyzer.NasaStats \
--master yarn \
<full-path-to-jar's-dir>/NasaWebAccessStats.jar <full-path-of-accessloganalyzer.conf> <full-path-of-output-dir-with-trailing-slash>
```

