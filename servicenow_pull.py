from ast import parse
from asyncio.windows_events import NULL
from operator import truediv
from pip._vendor import requests
import re
import sqlite3

def getSNTableName(url): # /<tableName>?
    return re.search('^.*?(?=\?)', re.search('([^/]+$)', url).group()).group() # first get everything after the final '/', then get everything before the question mark to extract the table name

def createLocalConnection(db):
    try:    
        conn = sqlite3.connect(db)
    except sqlite3.Error as e:
        print('createLocalConnection() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)
    return conn

def getSNRecords(url): # ServiceNow REST API Explorer code
    user = 'rest.read_user' # admin account so that it can view all records no matter ACLs / Query BRs
    pwd = 'RestReader123'
    headers = {'Content-Type':'application/json', 'Accept':'application/json'}
    response = requests.get(url, auth=(user, pwd), headers=headers )
    if response.status_code != 200: 
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
        exit()
    data = response.json()
    return data['result'] # return the data in a list

def getLocalRecords(conn, table):
    try:
        c = conn.cursor()
        c.execute('SELECT * FROM ' + table)
        snRecords = c.fetchall()
        return snRecords # return records from the sqlite database
    except sqlite3.Error as e:
        print('getLocalRecords() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def recordExists(conn, table, sysId):
    values = list()
    values.append(sysId)
    try:
        c = conn.cursor()
        c.execute('SELECT sys_id FROM ' + table + ' WHERE sys_id = ?', values)
        snRecords = c.fetchall()
        if(len(snRecords) > 0):
            return True;
        return False
    except sqlite3.Error as e:
        print('getLocalRecords() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def insertRecord(conn, table, record, snFieldNamesString):
    fieldsList = snFieldNamesString.split(',')
    values = list()
    qs = ''
    i = 0
    while i < len(fieldsList):
        qs += '?'
        if i < len(fieldsList) - 1: # add comma if not last item
            qs += ','
        i += 1
    j = 0
    while j < len(fieldsList):
        try:
            values.append(record[fieldsList[j]])
        except Exception as e:
            values.append(record[fieldsList[j] + '.sys_id'])
        j += 1
    try:    
        c = conn.cursor()
        c.execute('INSERT INTO ' + table + ' (' + snFieldNamesString + ') VALUES (' + qs + ')', values) # execute with the sql statement and a list of values in the same order as the local table columns
        conn.commit()
        print('insert: ' + values[1]) # print the second attribute from the database table
    except sqlite3.Error as e:
        print('insertRecord() - Error while connecting to sqlite.\nCheck that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\nSQLite message: ', e)

def updateRecord(conn, table, record, snFieldNamesString):
    fieldsList = snFieldNamesString.split(',')
    values = list()
    setString = ''
    i = 1
    while i < len(fieldsList):
        setString += fieldsList[i] + ' = ?'
        if i < len(fieldsList) - 1: # add comma if not last item
            setString += ','
        i += 1
    j = 1
    while j < len(fieldsList):
        try:
            values.append(record[fieldsList[j]])
        except Exception as e:
            values.append(record[fieldsList[j] + '.sys_id'])
        j += 1
    values.append(record[fieldsList[0]])
    try:    
        c = conn.cursor()
        c.execute('UPDATE ' + table + ' SET ' + setString + ' WHERE sys_id = ?', values) # execute with the sql statement and a list of values in the same order as the local table columns
        conn.commit()
        print('update: ' + values[0]) # print the first value from the databse object
    except sqlite3.Error as e:
        print('updateRecord() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def localTableExists(conn, table):
    values = list()
    values.append(table)
    try:    
        c = conn.cursor()
        c.execute('SELECT name FROM sqlite_master WHERE type=\'table\' AND name = ?', values)
        localTables = c.fetchall()
        if(len(localTables) == 0):
            return False
        return True
    except sqlite3.Error as e:
        print('localTableExists() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def parseSNFieldsForTableCreate(url): # Get a list of field names as it would appear in a CREATE TABLE statement (field_name TEXT,...)
    fields = url.split('sysparm_fields=')[1].split('&')[0].split('%2C') # first get everyhting after sysparm_fields=, then everyhting before &, then each field
    numFields = len(fields)
    fieldsSQL = ''
    i = 0
    while i < numFields:
        fieldType = 'TEXT' # TODO: detect DATETIME variables? would have to do a SELECT on each variable to run regex on a value from it
        fieldsSQL += fields[i].split('.')[0] + ' ' + fieldType
        if i < numFields - 1:
            fieldsSQL += ',' # field_1 TEXT, field_2 TEXT
        i += 1
    return fieldsSQL

def getSNFieldNamesCSV(url):
    return parseSNFieldsForTableCreate(url).replace(' TEXT', '')

def createLocalTable(conn, table, url):
    try:
        print('Table ' + '\'' + table + '\' does not exist, creating it...')
        c = conn.cursor()
        c.execute('CREATE TABLE ' + table + '(' + parseSNFieldsForTableCreate(url) + ')')
        print(table + ' table created')
    except sqlite3.Error as e:
        print('createLocalTable() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def doesFieldExistInLocalTable(conn, table, field):
    try:    
        c = conn.cursor()
        c.execute('PRAGMA table_info(' + table + ')') # Get a list of local field names
        localTableFields = c.fetchall()
        i = 0
        while i < len(localTableFields):
            if localTableFields[i][1] == field:
                return True
            i += 1
        return False
    except sqlite3.Error as e:
        print('doesFieldExistInLocalTable() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def createLocalFieldsIfNecessary(conn, table, snFieldNamesList): # snFieldNamesList is a python list()
    for name in snFieldNamesList:
        if doesFieldExistInLocalTable(conn, table, name) == False:
            print('creating new field '+ name + '...')
            try:
                c = conn.cursor()
                c.execute('ALTER TABLE ' + table + ' ADD ' + name + ' TEXT')
                print('new field created')
            except sqlite3.Error as e:
                    print('createLocalFieldsIfNecessary() - Error while connecting to sqlite. Check that any reference field in sysparm_fields ends with .sys_id (e.g. assignment_group.sys_id)\n', e)

def main(url):
    conn = createLocalConnection('servicenow.db') # create a connection to the sqlite database
    table = getSNTableName(url)
    print('Querying ServiceNow ' + table + ' table for the fields:')
    snFieldNamesList = getSNFieldNamesCSV(url).split(',')
    print(snFieldNamesList)

    if localTableExists(conn, table) == False: # if a table with the name parsed from the url does not in the sqlite data base then create one
        createLocalTable(conn, table, url)        
    else:
        createLocalFieldsIfNecessary(conn, table, snFieldNamesList) # Check the sn table vs local table and create fields if necessary

    for snRecord in getSNRecords(url): # Get records from ServiceNow, determine if the record exists, update or create a new record accordingly
        if recordExists(conn, table, snRecord['sys_id']) == True: 
            updateRecord(conn, table, snRecord, getSNFieldNamesCSV(url))
        else:
            insertRecord(conn, table, snRecord, getSNFieldNamesCSV(url))

    print('Update complete')
    conn.close()

main('https://dev12604.service-now.com/api/now/table/incident?sysparm_query=active%3Dtrue&sysparm_fields=sys_id%2Cnumber%2Cstate%2Cshort_description%2Csys_updated_on%2Cpriority%2Cimpact%2Curgency%2Cassignment_group.sys_id%2Cassigned_to.sys_id&sysparm_limit=10')