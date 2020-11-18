# DataChecker - Spark Application

Application for Data comparison between **original file** and **newly generated file** .
 - The original file is typically is **OLD file**. 
 - Newly generated file is typically EMR file.
 
```sh
 gradle clean build jar -x test ;
```

### Running the app
_Spark DataChecker_ is a **console** APP. User must use **4** or **5** parameters for running. Parameter should be with a --key 
1. **Original file** absolute p ath
2. **Checked file** absolute path
3. **Job name** e.g ( *AuWatch* , *Wallmart*, *WallMartCsv*)
4. Comma separated **columns to compare** . e.g (_marketCode,commercialDate,commercialTime_)
5. **[Optional]** Generated file absolute path. If parameter is absent application returns number of mismatched rows only

### Examples:
```sh
spark-submit --class com.tfedorov.datachecker.DataComparatorApp --master yarn --deploy-mode client SparkDataComparer-1.0-SNAPSHOT.jar --original s3://bucketname/Origin/Origin.CSV --checked s3://bucketname/new/WMTYYDDD_1.TXT/part-00000 --parser WallMartCsv --columns tvHouseHold --output s3://bucketname/new/result
```
1. Original file = **_s3://bucketname/origin/WMT20090.CSV_**
2. Checked file = **_s3://bucketname/new/WMTYYDDD_1.TXT/part-00000_**
3. Job name = **_WallMartCsv_**
4. Columns to compare = **_tvHouseHold_**
5.  **[Optional]** Result path = **_s3://bucketname/new/result**


Spark job looks at {Original file} & {Checked file}, parses files as WallMart Report, compares {Columns} and generates diff reports to {Result path}.
Also job generates number report to the Console.
Note. The original file is in CSV format, so we use WalMartCsv, not WalMart.

```sh
spark-submit --class com.tfedorov.datachecker.DataComparatorApp --master yarn --deploy-mode client SparkDataComparer-1.0-SNAPSHOT.jar --original s3://bucketname/origin/AuWatch.TXT --checked s3://bucketname/new/AuWatch.TXT --parser AuWatch --columns callLetter,commercialDescription,showTitle,showTypeDescription,dayPart,ultimateParentDesc,parentDesc,brandDesc,pccDesc,commercialDuration,expenditure,podNumber,podSequence,commercialTrafficType,brandVariantDesc,maxSequenceWithinPod
```
1. Original file = **_s3://bucketname/origin/AuWatch.TXT_**
2. Checked file = **_s3://bucketname/new/AuWatch.TXT_**
3. Service/Job name = **_AuWatch_**
4. Collumns to compare = **_callLetter,commercialDescription,showTitle,showTypeDescription,dayPart,ultimateParentDesc,parentDesc,brandDesc,pccDesc,commercialDuration,expenditure,podNumber,podSequence,commercialTrafficType,brandVariantDesc,maxSequenceWithinPod_**
5.  **[Optional]** Result path = **_s3://bucketname/new/result_**

Spark job looks at {Original file} & {Checked file}, parses files as AuWatch Report, compares {Columns} and generates number report to the Console
Note. There are no output file, so results would be on the console only.
