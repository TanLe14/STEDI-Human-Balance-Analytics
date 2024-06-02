CREATE EXTERNAL TABLE IF NOT EXISTS `tanlx_db`.`customer_landing` (
  `serialnumber` string,
  `sharewithpublicasofdate` string,
  `birthday` string,
  `registrationdate` string,
  `sharewithresearchasofdate` string,
  `customername` string,
  `email` string,
  `lastupdatedate` string,
  `phone` bigint,
  `sharewithfriendsasofdate` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tanlx-bucket/customer/landing/'
TBLPROPERTIES ('classification' = 'json');