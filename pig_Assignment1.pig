JSON:
--------
student1_json = load 'file:///home/orienit/Desktop/pig/student1.json' using JsonLoader('name:chararray,id:int,course:chararray,year:chararray');
dump student1_json
B = foreach student1_json generate $0,$1;
dump B
C = order student1_json BY $1 DESC
dump C


xml
---------
REGISTER '/home/orienit/Desktop/pig_ass_working/piggybank-0.15.0.jar'
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();
A = load 'file:///home/orienit/Desktop/pig/student1.xml' using org.apache.pig.piggybank.storage.XMLLoader('student') as (sdata:chararray);
B = foreach  A GENERATE XPath(sdata,'student/name'),XPath(sdata,'student/id'),XPath(sdata,'student/course'),XPath(sdata,'student/year');  
C = FILTER B BY id>2
student1_op_xml = FILTER B BY (int)$1 >2 and $2=='spark';
STORE student1_op_xml INTO 'file:///home/orienit/Desktop/pig_ass_working/op1xml'


