'''
Pendant Automation
Contact:(410) 939-7707
@author: Jeremy Scheuerman
@version: 1.5
Created:12/30/20
Last Updated:1/11/21
Changes:added support for config file and different day values for each table
Issues:
'''
import os
import sys
import datetime;
import python_config;
import string;

# get os stuff and file mod functions
import mysql.connector
from mysql.connector import connection


global truncate_table;
truncate_table="deploy.truncation_log";
#global variables

# database file located dat_converter/database file
config=python_config.read_db_config('config.ini','mysql');
deploy_db_user = user = config.get('user')
deploy_db_pass = config.get('password');
deploy_db_host = config.get('host');
config=python_config.read_db_config('config.ini','db_truncate');
temp=config.get('values');
#get dbtables as array
db_values=temp.split(',');
db_days=[]
db_tables={}
i=0;
j=0;
for i in range(len(db_values)-1):
    #for amount of values
    #seprate tables
    if (i+2)%2==0:
        db_tables[str(db_values[i])]=str(db_values[i+1]);
    #seperate number of days
    i+=1;
cnct = connection.MySQLConnection(user=deploy_db_user, password=deploy_db_pass, host=deploy_db_host);                                                        
# establish connection names are temporary until mysql is figured out
print("Connected to database succesfully");
#get cursor
mycursor = cnct.cursor();


def log_table_create():
    # function to create a TruncateLogs table if needed
    mycursor.execute("USE deploy");
    # create log table
    mytable = """CREATE TABLE IF NOT EXISTS truncation_log
    (Source VARCHAR(20) ,type VARCHAR(20), Message VARCHAR(100) NOT NULL,
    TIMESTAMP datetime(3));"""
    # create table and commit to database
    mycursor.execute(mytable);
    cnct.commit();
    print(truncate_table+" created succesfully");  

def dat_log_insert(num_records, table_name,exception=0,*e):
    current_timestamp=str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]);
    sql = ("INSERT INTO " + truncate_table + """ (Source,Type,Message,TimeStamp) VALUES (%s,%s,%s,%s)""");
    # setup table insertion
    val = ("Data Truncation",str(table_name),str(num_records)+" records were deleted from "+str(table_name),current_timestamp);
    # setup values for insertion#if an exception has been raised
    if exception==1:
          val = ("Data Truncation","Exception","an exception "+str(e)+" has occured",current_timestamp);
    # insert the data into the table
    mycursor.execute(sql, val);
    # commit to database
    cnct.commit();

def truncate(table,time):
    # deletes data older than specified days and updates the id columns
    tables = [];
    i = 0;
    j = 0;
    # initilize empty table array
    # get table names
    try:
        rec_count=0;
        mycursor.execute("SELECT * FROM "+table);
        mycursor.fetchall()
        rec_count=str(mycursor.rowcount);
        rec_count=int(rec_count);
    except Exception as e:
        dat_log_insert(rec_count,table,1,str(e));
    try:
        mycursor.execute("DELETE FROM " + table + " WHERE updated_at IS NOT null and updated_at < NOW() - INTERVAL "+ str(time) +" DAY LIMIT 100000;");
        mycursor.execute("select * from " + table);
        mycursor.fetchall();
    # fetch all so we can get rows
        rows = str(mycursor.rowcount);
        rows = int(rows);
        rec_count-=rows;
        try:
            if rec_count!=0:
                # cast to string and then back to int so we can read the data
                dat_log_insert(rec_count,table);
        except:
            print("The truncation log table does not exist or the tables were not formatted correctly, logging an exception");
        #if there are no records to deleted
        if rec_count!=0:
        # provide logging
            print(table + " is being cleaned of "+str(rec_count) +" records  older than "+str(time)+" days");
        else:
            print(table+" has no (old) data to be cleaned");
    except:
        print("The updated at field is null, does not exist,or was incorrectly formatted the "+table+" table has been skipped");
    finally:
        print("The operation on "+table +" has been completed" );
    cnct.commit();
#db Variables
# specify your list of databases
# insert tables here in the arrays

for table,days in db_tables.items():
    #truncates tables specified within database
    truncate(table,days);
    #deletes tables per each database

cnct.close();
sys.exit();
