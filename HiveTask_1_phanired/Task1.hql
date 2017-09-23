sudo cp /home/orienit/Desktop/hive-serdes-1.0-SNAPSHOT.jar /usr/lib/hive/lib/
sudo cp /home/orienit/Desktop/hivexmlserde-1.0.5.3.jar /usr/lib/hive/lib/

hive;

ADD JAR /usr/lib/hive/lib/hive-serdes-1.0-SNAPSHOT.jar;
ADD JAR /usr/lib/hive/lib/hivexmlserde-1.0.5.3.jar;

drop table student1_json;

CREATE EXTERNAL TABLE IF NOT EXISTS student1_json(name string,id int, course string, year string)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe' LOCATION '/user/cloudera/phanired/json';


drop table student1_jop;

create table student1_jop
as
select * from student1_json where id >2 and course='spark';

select * from student1_jop;

====================================================================================================================

DROP TABLE IF EXISTS student1_xml;

CREATE EXTERNAL TABLE IF NOT EXISTS student1_xml(str string)
 LOCATION '/user/cloudera/phanired/xml';



select * from student1_xml;

DROP TABLE IF EXISTS  student1_xop;
CREATE TABLE student1_xop STORED AS ORC AS
    SELECT xpath_string(str,'student/name')  AS name, 
           xpath_string(str,'student/id') AS id,
           xpath_string(str,'student/course') AS course,
           xpath_string(str,'student/year') AS year
      FROM student1_xml;
	  
select * from student1_xop;

create table student1_xop1
as
select * from student1_xop where id >2 and course='spark';

select * from student1_xop1;
