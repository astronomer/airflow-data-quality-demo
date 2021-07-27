CREATE TABLE IF NOT EXISTS {{ var.json.aws_configs.redshift_table }}
(ID int,Y int,month varchar,day varchar,FFMC float,DMC float,DC float,ISI float,temp float,RH float,wind float,rain float,area float);
