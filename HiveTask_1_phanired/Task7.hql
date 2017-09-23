hadoop fs -put Behavioral_Risk_Factor_Surveillance_System2.csv /user/cloudera/phanired;

drop table riskfactor;

CREATE TABLE riskfactor (
  yearstart int,
  yearend int,
  locationabbr string,
  locationdesc string,
  datasource string,
  class string,
  topic string,
  question string,
  data_value_unit string,
  data_value_type string,
  data_value int,
  data_value_alt int,
  data_value_footnote_symbol string,
  data_value_footnote string,
  low_confidence_limit decimal,
  high_confidence_limit decimal,
  sample_size_total int,
  age_in_years int,
  education string,
  gender string,
  income string,
  race_ethnicity string,
  geolocation decimal,
  classid string,
  topicid string,
  questionid string,
  datavaluetypeid string,
  locationid int,
  stratificationcategory1 string,
  stratification1 string,
  stratificationcategoryid1 string,
  stratificationid1 string
)
row format delimited fields terminated by ',';

LOAD DATA INPATH '/home/Behavioral_Risk_Factor_Surveillance_System.csv' OVERWRITE INTO TABLE riskfactor;

set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

drop table riskfactor_partition;
CREATE TABLE IF NOT EXISTS riskfactor_partition (
  yearstart int,
  locationabbr string,
  datasource string,
  class string,
  topic string,
  question string,
  data_value_unit string,
  data_value_type string,
  data_value int,
  data_value_alt int,
  data_value_footnote_symbol string,
  data_value_footnote string,
  low_confidence_limit decimal,
  high_confidence_limit decimal,
  sample_size_total int,
  age_in_years int,
  education string,
  gender string,
  income string,
  race_ethnicity string,
  classid string,
  topicid string,
  questionid string,
  datavaluetypeid string,
  locationid int,
  stratificationcategory1 string,
  stratification1 string,
  stratificationcategoryid1 string,
  stratificationid1 string)
PARTITIONED BY (yearend int ,locationdesc string,geolocation decimal)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\,'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

INSERT OVERWRITE TABLE riskfactor_partition PARTITION(yearend,locationdesc,geolocation) select * from riskfactor;

SHOW PARTITIONS riskfactor_partition;