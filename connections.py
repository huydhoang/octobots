import cx_Oracle
import pyodbc

from collections import namedtuple
from aes_encryption import *
from TableIt import printTable
from utils import *
from sys import exit

Oracle = namedtuple('Oracle', ('username','password','dsn'))
MSSQL = namedtuple('MSSQL', ('driver', 'server','database', 'username', 'password'))

# Connection objects are connection info as namedtuples
def get_connection_object(connArray):
    if connArray[1] == 'Oracle':
        connObject = Oracle(*connArray[2:])
    elif connArray[1] == 'MSSQL':
        connObject = MSSQL(*connArray[2:])
    return connObject

# Get connType and connUser
def get_connection_info(connObject):
    connType = type(connObject).__name__
    connSysUser = None
    if connType == 'Oracle':
        connSysUser = connObject.username
    elif connType == 'MSSQL':
        connSysUser = connObject.database
    return connType, connSysUser

# Load data from file
def load_from_file(filename):
    # open with append permission, create if not existed
    with open(filename,"a+") as cxnsFile:
        pass

    # read cxnsFile
    try:
        with open(filename,"r") as cxnsFile:
            connections = cxnsFile.readlines()
    except IOError as e:
        print("[ERROR!!!]", e)
        return None

    # decrypt and strip any '\n' tail on the right
    connections = [decrypt(connection.rstrip('\n')) for connection in connections]
    # from string to list
    connections = [connection.split(',') for connection in connections]
    # from list to dictionary
    connections = {connection[0].upper() : get_connection_object(connection) for connection in connections}

    return connections

# Save data to file
def save_to_file(connDict, filename):
  # save connections
  if connDict.keys() != []:
    try:
        with open(filename,"w") as cxnsFile:
            for connName in connDict.keys():
                # generate a csv-style connection string to save
                # name, connection type, items in connObject
                row = connName + ',' + type(connDict.get(connName)).__name__ + ',' + ','.join([str(item) for item in list(connDict.get(connName))]) 
                cxnsFile.write(encrypt(row)+'\n')
        return None
    except IOError as e:
        print("[ERROR!!!]", e)
        return e


# test connection
def test_connect(connName, connObject):
    connType = type(connObject).__name__
    Connection = None
    if connType == 'Oracle':
        try:
            Connection = cx_Oracle.connect(user=connObject.username, password=connObject.password, dsn=connObject.dsn, encoding="UTF-8", nencoding="UTF-8")
            print(f'Success: Connected to {connName}. Db version: {Connection.version}')
            Connection.close()
            return None
        except cx_Oracle.DatabaseError as e:
            print("[ERROR!!!]", e)
            return e
    elif connType == 'MSSQL':
        try:
            Connection = pyodbc.connect('Driver={'+connObject.driver+'};'
                                        'Server='+connObject.server+';'
                                        'Database='+connObject.database+';'
                                        'UID='+connObject.username+';'
                                        'password='+connObject.password+';'
                                        'Trusted_Connection=yes;')
            print(f'Success: Connected to {connName}. Driver: {connObject.driver}')
            Connection.close()
            return None
        except pyodbc.Error as e:
            print("[ERROR!!!]", e)
            return e

def create_new_conn(connDict):
    printViewTitle('CREATE A NEW CONNECTION')
    # get user inputs for new connection
    connArray = []
    while connArray == []:
        connName = input("Connection name: ")
        while connName.upper() in ['LST_SOURCE', 'LST_TARGET']:
            print("[ERROR!!!] Reserved system name.")
            connName = input("Choose a different name: ")
        while connDict.get(connName.upper(), None) != None:
            connName = input("Connection name existed! Choose a new name: ")
        # Prompt for connection type
        connType = input("The following connection types are supported:\n(1) Oracle\n(2) MSSQL\nEnter a number: ")
        # Oracle parameters
        if connType.upper() in ['1','ORACLE']:
            connUser = input("Username: ")
            connPass = input("Password: ")
            connDsn  = input("DSN (ip_address:port/service): ")
            connArray = [connName, 'Oracle', connUser, connPass, connDsn]
        # MSSQL parameters
        elif connType.upper() in ['2','MSSQL']:
            connDriver = input("Driver (Default='SQL Server'): ")
            if connDriver == '':
                connDriver = 'SQL Server'
            connServer = input("Server: ")
            connDatabase = input("Database: ")
            connUser = input("Username: ")
            connPass = input("Password: ")
            connArray = [connName, 'MSSQL', connDriver, connServer, connDatabase, connUser, connPass]
    # test connection
    connObject = get_connection_object(connArray)
    connErr = test_connect(connName, connObject)
    
    # append if not existed
    getConn = connDict.get(connName.upper(), None)
    fileErr = None
    if connErr == None and getConn == None:
        # remove last source and target from connDict if existed
        last_source = connDict.get('LST_SOURCE', None)
        last_target = connDict.get('LST_TARGET', None)
        for item in ['LST_SOURCE', 'LST_TARGET']:
            if connDict.get(item, None) != None:
                connDict.pop(item)
        # add new connection to connDict
        connDict[connName.upper()] = get_connection_object(connArray)
        # re-add last source and target to connDict
        connDict['LST_SOURCE'] = last_source
        connDict['LST_TARGET'] = last_target
        # save to file
        fileErr = save_to_file(connDict, filename='cxns')
    return connDict, connErr, fileErr


# View connections
def view_connections(connDict):
    i = 0
    cxnsTable = [['id', 'name', 'type', 'server address', 'username']]
    for key in connDict.keys():
        i += 1
        item = connDict.get(key, None)
        connType = type(item).__name__
        if connType == 'Oracle':
            connDsn = item.dsn
        elif connType == 'MSSQL':
            connDsn = item.server + '/' + item.database
        if key not in ['LST_SOURCE', 'LST_TARGET']:
            cxnsTable.append([f'{i}', key, connType, connDsn, item.username])
        # print(f"({i}) {key} : type={connType}, dsn={connDsn}, username={item.username}")
    numSaved = len(cxnsTable) - 1
    print(f"Found {numSaved} saved connections.")
    if numSaved > 0:
        printTable(cxnsTable, useFieldNames=True)
    return numSaved

# Select connection
def select_connection(connDict):
    # view_connections(connDict)
    keyMap = dict()
    i = 0
    for key in connDict.keys():
        i += 1
        keyMap[i] = key

    option = None
    while option not in keyMap.keys():
        option = input("Connection id: ")
        if option.isnumeric():
            option = int(option)

    connName = keyMap.get(option, None)
    connObject = connDict.get(connName, None)
    connType, connUser = get_connection_info(connObject)

    return connName, connObject

# Modify connection
def modify_connection(connDict):
    printViewTitle('MODIFY CONNECTION')
    connName, connObject = select_connection(connDict)
    connType = type(connObject).__name__
    print(f"Enter new values for {connName}: ")
    if connType == 'Oracle':
        connUser = get_new_input(connObject.username, f"Username: (Current={connObject.username}) ")
        connPass = get_new_input(connObject.password, f"Password: (Current=******) ")
        connDsn  = get_new_input(connObject.dsn, f"DSN (Current={connObject.dsn}): ")
        connArray = [connName, connType, connUser, connPass, connDsn]
    elif connType == 'MSSQL':
        connDriver = get_new_input(connObject.driver, f"Driver: (Current={connObject.driver}) ")
        connServer = get_new_input(connObject.server, f"Server: (Current={connObject.server}) ")
        connDatabase = get_new_input(connObject.database, f"Database: (Current={connObject.database}) ")
        connUser = get_new_input(connObject.username, f"Username: (Current={connObject.username}) ")
        connPass = get_new_input(connObject.password, f"Password: (Current=******) ")
        connArray = [connName, connType, connDriver, connServer, connDatabase, connUser, connPass]
    # test connection
    connObject = get_connection_object(connArray)
    connErr = test_connect(connName, connObject)
    # save
    fileErr = None
    if connErr == None:
        connDict[connName] = get_connection_object(connArray)
        # save to file
        fileErr = save_to_file(connDict, filename='cxns')
    return connDict, connErr, fileErr

# Delete connection
def delete_connection(connDict):
    printViewTitle('DELETE CONNECTION')
    connName, connObject = select_connection(connDict)
    print(f"{connName} is going to be deleted!")
    textInput = ''
    confirmed = False
    while textInput.upper() not in ['YES', 'NO']:
        textInput = input("Are you sure? (yes/no): ")
        confirmed = (textInput.upper() == 'YES')
    if confirmed:
        connDict.pop(connName)
        print(f"{connName} deleted!")
    else:
        # go back to manage_connections loop
        pass
    # save
    fileErr = None
    fileErr = save_to_file(connDict, filename='cxns')
    return connDict, fileErr

# Manage connections
def manage_connections(connDict):
    printViewTitle('MANAGE CONNECTIONS')
    option = None
    while option != 4:
        numSaved = view_connections(connDict)
        if numSaved > 0:
            print("What would you like to do?\n (1) Create new connection\n (2) Modify connection\n (3) Delete connection\n (4) Done")
            
            while option not in range(1,5):
                option = input("Enter an option number: ")
                if option.isnumeric():
                    option = int(option)
        else:
            # if 0 saved connections, jump directly to create one
            print("Must create a connection to continue!")
            option = 1
        
        connErr = None
        fileErr = None
        if option == 1:
            connDict, connErr, fileErr = create_new_conn(connDict)
        elif option == 2:
            connDict, connErr, fileErr = modify_connection(connDict)
        elif option == 3:
            connDict, fileErr = delete_connection(connDict)
        elif option == 4:
            print("Choose source and target to continue!")
            break
        option = None
    return option

# Save last connections to file
def save_last_connections(connDict, last_source, last_target):
    # add/update last source and target to connDict
    connDict['LST_SOURCE'] = last_source
    connDict['LST_TARGET'] = last_target
    fileErr = save_to_file(connDict, filename='cxns')
    return fileErr

# connections = load_from_file(filename="cxns")
# driver='SQL Server', server='10.99.2.63', database='TMP', uid='', password=''
# 'Oracle', 'PFS_STAGING', 'uUn4PNcX\Jb-eA5_', '10.101.5.143:1521/awbtest'


# manage_connections(connections)

def select_source_target():
    # read from file
    connections = load_from_file(filename="cxns")
    source = None
    target = None
    
    last_source = connections.get('LST_SOURCE', None)
    last_target = connections.get('LST_TARGET', None)

    changeConn = None
    lsourceSysUser = None
    ltargetSysUser = None
    if last_source != None and last_target != None:
        lsourceType, lsourceSysUser = get_connection_info(last_source)
        ltargetType, ltargetSysUser = get_connection_info(last_target)
        print(f"Last source: {lsourceType}/{lsourceSysUser}")
        print(f"Last target: {ltargetType}/{ltargetSysUser}")
        source = last_source
        target = last_target
        test_connect(lsourceSysUser, source)
        test_connect(ltargetSysUser, target)
        print("Using last source/target.")
        while changeConn not in ['', 'Y', 'N']:
            changeConn = input("Keep using last source/target? (Y/n): ").upper()
        

    if connections == {} or changeConn not in ['', 'Y']:
        option = None
        if connections == {}:
            option = 2
        while option not in [1, 2, 3]:
            print("Select an option below:\n (1) Select data source/target\n (2) Manage connections\n (3) Exit")
            option = input("Enter an option number: ")
            if option.isnumeric():
                option = int(option)
        while option in [1, 2, 3]:
            if option == 1:
                view_connections(connections)
                print("[Select a source]")
                sourceName, source = select_connection(connections)
                sourceType, sourceSysUser = get_connection_info(source)
                print("[Select a target]")
                targetName, target = select_connection(connections)
                targetType, targetSysUser = get_connection_info(target)
                test_connect(sourceSysUser, source)
                test_connect(targetSysUser, target)
                option = None
            elif option == 2:
                manage_connections(connections)
                option = 1
            elif option == 3:
                exit()
    fileWriteErr = None
    if connections != {} and source and target:
        fileWriteErr = save_last_connections(connections, source, target)
    return source, target, fileWriteErr