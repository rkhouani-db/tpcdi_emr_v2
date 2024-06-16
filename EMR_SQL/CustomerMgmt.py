import os
import urllib.request
import json
import argparse
import boto3
from pyspark.sql import SparkSession

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--scale_factor', type=int, required=True)
parser.add_argument('--wh_db', type=str, required=True)
parser.add_argument('--bucketname', type=str, required=True)

# Read arguments from command line
args, unknown = parser.parse_known_args()

scale_factor = args.scale_factor
wh_db=args.wh_db+"_"+str(scale_factor)
staging_db = wh_db+"_stage"
bucketname = args.bucketname

spark = SparkSession\
        .builder\
        .appName("helper app")\
        .enableHiveSupport()\
        .getOrCreate()




# URL of the JSON file
url = "https://raw.githubusercontent.com/shannon-barrow/databricks-tpc-di/d0e8434cca224fbd43ef395c015ab57c87652fc8/src/tools/traditional_config.json"

# Use urllib to download the file
with urllib.request.urlopen(url) as response:
    file = response.read()

# Parse the file contents as a JSON dictionary
data = json.loads(file)

# Now you can use the data as a normal dictionary in Python
table_conf = data["views"]["CustomerMgmt"]

user_name = (
    spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".", "_")
)

files_directory = f"s3://{bucketname}/tpcdi_data/sf={scale_factor}/"
print(f"Loading xmls from {files_directory}/{table_conf['path']}/{table_conf['filename']}")
spark.read.format("xml").options(rowTag=table_conf["rowTag"], inferSchema=False).load(
    f"""{files_directory}/{table_conf['path']}/{table_conf['filename']}"""
).createOrReplaceTempView("v_CustomerMgmt")

# Create/Drop/Create DB to clean out the underlying folders
spark.sql(f"CREATE DATABASE IF NOT EXISTS {staging_db} location 's3://{bucketname}/tpcdi_data/databases/{staging_db}'")
spark.sql(f"drop database {staging_db} cascade")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {staging_db} location 's3://{bucketname}/tpcdi_data/databases/{staging_db}'")

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
spark.sql(
    f"""
  CREATE TABLE {staging_db}.CustomerMgmt using delta PARTITIONED BY (ActionType) 
AS SELECT
    cast(Customer._C_ID as BIGINT) customerid,
    cast(Customer.Account._CA_ID as BIGINT) accountid,
    cast(Customer.Account.CA_B_ID as BIGINT) brokerid,
    nullif(Customer._C_TAX_ID, '') taxid,
    nullif(Customer.Account.CA_NAME, '') accountdesc,
    cast(Customer.Account._CA_TAX_ST as TINYINT) taxstatus,
    decode(_ActionType,
      "NEW","Active",
      "ADDACCT","Active",
      "UPDACCT","Active",
      "UPDCUST","Active",
      "CLOSEACCT","Inactive",
      "INACT","Inactive") status,
    nullif(Customer.Name.C_L_NAME, '') lastname,
    nullif(Customer.Name.C_F_NAME, '') firstname,
    nullif(Customer.Name.C_M_NAME, '') middleinitial,
    nullif(upper(Customer._C_GNDR), '') gender,
    cast(Customer._C_TIER as TINYINT) tier,
    cast(Customer._C_DOB as DATE) dob,
    nullif(Customer.Address.C_ADLINE1, '') addressline1,
    nullif(Customer.Address.C_ADLINE2, '') addressline2,
    nullif(Customer.Address.C_ZIPCODE, '') postalcode,
    nullif(Customer.Address.C_CITY, '') city,
    nullif(Customer.Address.C_STATE_PROV, '') stateprov,
    nullif(Customer.Address.C_CTRY, '') country,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_1.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_1.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_1.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_1.C_EXT, '')),
      cast(null as string)) phone1,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_2.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_2.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_2.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_2.C_EXT, '')),
      cast(null as string)) phone2,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_3.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_3.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_3.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_3.C_EXT, '')),
      cast(null as string)) phone3,
    nullif(Customer.ContactInfo.C_PRIM_EMAIL, '') email1,
    nullif(Customer.ContactInfo.C_ALT_EMAIL, '') email2,
    nullif(Customer.TaxInfo.C_LCL_TX_ID, '') lcl_tx_id,
    nullif(Customer.TaxInfo.C_NAT_TX_ID, '') nat_tx_id,
       to_timestamp(_ActionTS) update_ts,
    _ActionType ActionType
  FROM v_CustomerMgmt
"""
)



