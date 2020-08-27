# Rudimentary-Database-Managenent-System
A rudimentary database management system made in python which executes a few DDL, DQL and DML commands.

Readme file for DavisBase Application.
Language: Python 3.6.
Date: 4th May 2020
OS: Linux/Windows
=====Description=====:
Davisbase is a limited applcation that allows for select, create, drop and insert for tables.

====Pre-requisites====:
Packages needed to be downloaded is sqlparse.
To install sqlparse:
pip install sqlparse

To run the application:
python DavisBase.py

Some Examples:
For creating a table:
	create table test ( a int primary key, b text );
To insert values in table:
	insert into test ( a , b) values ( 1, 2);
To view the table:
	select * from test;
To drop the table:
	drop table test;
To show all tables:
	show tables;
