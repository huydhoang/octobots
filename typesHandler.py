import configparser
from TableIt import printTable
from decimal import Decimal
# from datetime import datetime
from utils import *
from sys import exit
import codecs
from cx_Oracle import connect as oraconnect


def get_mappingDict(mapTitle):
    # printViewTitle("DATA MAPPING")
    config = configparser.ConfigParser()
    # create file if not existed
    with open('datatypes.ini', 'a+') as configfile:
        pass
    # read from file
    with codecs.open('datatypes.ini', 'r+', 'utf8') as configfile:
        config.read_file(configfile)
        try:
            mappings = config[mapTitle]
        except:
            printLog(f"[ERROR!!!] Could not find section [{mapTitle}] in 'datatypes.ini'!", LOGFILE)
            printLog(f"[SOLUTION] Add a new section titled [{mapTitle}] to fix this!", LOGFILE)
            exit()
        mappingDict = {}
        for key in mappings:
            mappingDict[key] = mappings[key]
    
    # print(mappingDict)
    return mappingDict


def sqlCreateBuilder(source, target, tableAddress, meta, mappingDict):


    sourceType = type(source).__name__
    targetType = type(target).__name__
    mapTitle = f"{sourceType} --> {targetType}"

    ORADB_VERSION = None
    if targetType == 'Oracle':
        schemaName = target.username
        # Get oradb version to check for column name length limit
        throwawayConn = oraconnect(user=target.username, password=target.password, dsn=target.dsn)
        ORADB_VERSION = throwawayConn.version.split('.')
        ORADB_VERSION = [int(i) for i in ORADB_VERSION if i.isnumeric()]
        throwawayConn.close()
        # print(ORADB_VERSION)
        # exit()

    elif targetType == 'MSSQL':
        schemaName = target.database
    tableName = tableAddress.split('.')[1]

    # return source datatypes for saving
    sourceDTypes = ''

    # generate target columns for saving
    columnNames = []
    targetColumns = ''

    # generate a meta string for building sqlCreate & sqlInsert commands
    metaString = ''

    # COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
    targetDtypeTable = [['id', 'source: column_name', f'source: {sourceType}', f'target: {targetType}']]
    dtypeDict = dict()
    i = 0
    for row in meta:
        # print(row)
        i += 1
        COLUMN_NAME = row[0]
        SOURCE_DTYPE = row[1]

        # add type to sourceDTypes
        sourceDTypes += f"{SOURCE_DTYPE}, "

        if sourceType != targetType:
            # get mapping from profiles.ini
            try:
                TARGET_DTYPE = mappingDict[SOURCE_DTYPE.lower()]
            except:
                printLog(f"[ERROR!!!] Could not get datatype mapping from 'datatypes.ini'!", LOGFILE)
                printLog(f"[SOLUTION] Add a mapping for '{SOURCE_DTYPE}' under [{mapTitle}] to fix this!", LOGFILE)
                exit()
        else:
            TARGET_DTYPE = SOURCE_DTYPE

        # add item to table and dict
        targetDtypeTable.append([f'{i}', COLUMN_NAME, SOURCE_DTYPE, TARGET_DTYPE])
        dtypeDict[i] = [COLUMN_NAME, TARGET_DTYPE]
        columnNames.append(COLUMN_NAME)
    

    # update meta with mapped target datatype
    for i in range(0, len(meta)):
        row = list(meta[i])
        COLUMN_NAME = row[0]
        SOURCE_DTYPE = row[1]
        DATA_LENGTH = row[2]
        DATA_PRECISION = row[3]
        DATA_SCALE = row[4]
        NULLABLE = row[5]
        CHAR_LENGTH = row[6]

        # modify metadata for compatibility
        if mapTitle == 'MSSQL --> Oracle':
            # In Oracle 12.2 and above the maximum object name length is 128 bytes.
            # In Oracle 12.1 and below the maximum object name length is 30 bytes.
            if (ORADB_VERSION[0] <= 11 or (ORADB_VERSION[0] == 12 and ORADB_VERSION[0] <= 1)):
                while len(COLUMN_NAME) > 30:
                    COLUMN_NAME = input(f"Column '{COLUMN_NAME}': Name must be <= 30 characters! Rename column: ")
                    if COLUMN_NAME in columnList:
                        print("Column name existed! Choose a new name...")
                        COLUMN_NAME = '0'*150
            else:
                while len(COLUMN_NAME) > 128:
                    COLUMN_NAME = input(f"Column '{COLUMN_NAME}': Name must be <= 128 characters! Rename column: ")
                    if COLUMN_NAME in columnList:
                        print("Column name existed! Choose a new name...")
                        COLUMN_NAME = '0'*150
            
            # in Oracle, column name must be unsigned and uppercase
            row[0] = return_unsigned(COLUMN_NAME).upper()

            # in Oracle, type VARCHAR2 has hard limit of 4000 characters
            if SOURCE_DTYPE in ['varchar', 'nvarchar'] and DATA_LENGTH == -1:
                DATA_LENGTH = 4000
                row[2] = DATA_LENGTH
                CHAR_LENGTH = 4000
                row[6] = CHAR_LENGTH

        # from Oracle to MSSQL
        elif mapTitle == 'Oracle --> MSSQL':
            # In SQL Server, column name length limit is 128 bytes
            while len(COLUMN_NAME) > 128:
                COLUMN_NAME = return_unsigned(input(f"Column '{COLUMN_NAME}': Name must be <= 128 characters! Rename column: ")).upper()
                if COLUMN_NAME in columnList:
                    print("Column name existed! Choose a new name...")
                    COLUMN_NAME = '0'*150
            
            row[0] = COLUMN_NAME
            
            # pre-processs some data types
            if SOURCE_DTYPE in ['CLOB', 'NCLOB', 'LONG']:
                DATA_LENGTH = 'max'
                row[2] = DATA_LENGTH
            elif SOURCE_DTYPE.find('TIME ZONE') != -1:
                DATA_LENGTH = 37
                row[2] = DATA_LENGTH
            elif SOURCE_DTYPE.find('TIMESTAMP') != -1:
                DATA_LENGTH = 7
                row[2] = DATA_LENGTH
        
        # from MSSQL to MSSQL
        elif mapTitle == 'MSSQL --> MSSQL':
            printLog(f"{mapTitle} is not supported yet!", LOGFILE)
            # keep the window open until user press Enter
            input("\nPress Enter to close...")

        # update datatype value
        TARGET_DTYPE = dtypeDict[i + 1][1]
        row[1] = TARGET_DTYPE

        # put row back in meta
        meta[i] = tuple(row)

    if sourceType != targetType:
        print("Different target datatypes detected. Please confirm!")
        printTable(targetDtypeTable, useFieldNames=True)
        textInput = None
        while textInput != 'YES':
            textInput = input(f"Enter 'yes' to confirm target types or column id to modify: ").strip().upper()
            selectedId = None
            if textInput.isnumeric(): 
                selectedId = int(textInput)
            if selectedId in dtypeDict.keys():
                COLUMN_NAME = dtypeDict[selectedId][0]
                TARGET_DTYPE = dtypeDict[selectedId][1]
                newValue = input(f"{COLUMN_NAME}: Type = '{TARGET_DTYPE}'. New value: ").strip().upper()
                if newValue != '':
                    # update value in meta, selectedId - 1 to match array indexing (which starts from 0)
                    meta[selectedId - 1][1] = newValue
                    print("datatype updated!")
                else:
                    print("Value unchanged.")
    else:
        print("Target datatypes fetched 1:1 from source.")

    # build metaString
    for row in meta:
        COLUMN_NAME = row[0]
        TARGET_DTYPE = row[1]
        DATA_LENGTH = row[2]
        DATA_PRECISION = row[3]
        DATA_SCALE = row[4]
        NULLABLE = row[5]
        CHAR_LENGTH = row[6]

        if targetType == 'Oracle':
            # add column names to targetColumns for saving into profile
            targetColumns += f'"{COLUMN_NAME}", '

            # handle common Datatypes that require no length specified
            if TARGET_DTYPE in [
                'DATE', 'TIMESTAMP(6)', 'TIMESTAMP(6) WITH TIME ZONE',
                'TIMESTAMP(1)', 'TIMESTAMP(1) WITH TIME ZONE',
                'TIMESTAMP(2)', 'TIMESTAMP(2) WITH TIME ZONE',
                'TIMESTAMP(3)', 'TIMESTAMP(3) WITH TIME ZONE',
                'TIMESTAMP(4)', 'TIMESTAMP(4) WITH TIME ZONE',
                'TIMESTAMP(5)', 'TIMESTAMP(5) WITH TIME ZONE',
                'TIMESTAMP(7)', 'TIMESTAMP(7) WITH TIME ZONE',
                'TIMESTAMP(8)', 'TIMESTAMP(8) WITH TIME ZONE',
                'TIMESTAMP(9)', 'TIMESTAMP(9) WITH TIME ZONE',
                'CLOB', 'NCLOB', 'BLOB', 'BFILE', 'ANYDATA',
                'INT', 'REAL', 'FLOAT', 'LONG', 'LONG RAW', 'UROWID'
                ]:
                metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}, '
            # handle NUMBER Datatypes
            elif TARGET_DTYPE == 'NUMBER':
                # The Oracle NUMBER data type has precision and scale.
                # NUMBER[(precision [, scale])]
                # Found some columns on data warehouse where scale = null but there were floating points
                # and inserted data lost floating points. Added a scale of 21 to fix. (dwh prod values followed this scale)
                if DATA_PRECISION == None:
                    if DATA_SCALE == None:
                        metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}(*,21), '
                    else:
                        metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}(*,{DATA_SCALE}), '
                else:
                    if DATA_SCALE == None:
                        metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_PRECISION},21), '
                    else:
                        metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_PRECISION},{DATA_SCALE}), '

            # handle Character Datatypes
            elif TARGET_DTYPE == 'NCHAR' and DATA_LENGTH <= 1000:
                metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_LENGTH}), '
            elif TARGET_DTYPE == 'RAW' and DATA_LENGTH <= 2000:
                metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_LENGTH}), '
            elif TARGET_DTYPE == 'CHAR' and DATA_LENGTH <= 2000:
                metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_LENGTH} BYTE), '
            elif TARGET_DTYPE in ['VARCHAR', 'VARCHAR2'] and DATA_LENGTH <= 4000:
                metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_LENGTH} BYTE), '
            elif TARGET_DTYPE in ['NVARCHAR2'] and DATA_LENGTH <= 4000:
                metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({CHAR_LENGTH}), '
            # catch any missing datatypes
            else:
                printLog(f"[WARNING] Encountered undefined datatype for {COLUMN_NAME}: type='{TARGET_DTYPE}', length={DATA_LENGTH}!", LOGFILE)
                printLog("Values must be set manually to ensure compatibility!", LOGFILE)
                TARGET_DTYPE = input("Set target DATA_TYPE: ").strip()
                DATA_LENGTH = input("Set target DATA_LENGTH: ").strip()
                DATA_SCALE = input("Set target DATA_SCALE: ").strip()
                if DATA_LENGTH == '':
                    metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}, '
                elif DATA_SCALE == '':
                    metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_LENGTH}), '
                else:
                    metaString += f'"{COLUMN_NAME}" {TARGET_DTYPE}({DATA_LENGTH},{DATA_SCALE}), '
        
        elif targetType == 'MSSQL':
            # add column names to targetColumns for saving into profile
            targetColumns += f'[{COLUMN_NAME}], '

            # handle MSSQL data types
            if TARGET_DTYPE.upper() in ['FLOAT']:
                metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}], '
            elif TARGET_DTYPE.upper() in ['VARCHAR', 'NVARCHAR']:
                metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}]({DATA_LENGTH}), '
            elif TARGET_DTYPE.upper()  in ['DATETIME', 'DATETIME2']:
                metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}]({DATA_LENGTH}), '
            elif TARGET_DTYPE.upper()  in ['NUMERIC'] and DATA_SCALE == None:
                metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}]({DATA_PRECISION}), '
            elif TARGET_DTYPE.upper()  in ['NUMERIC'] and DATA_SCALE != None:
                metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}]({DATA_PRECISION},{DATA_SCALE}), '
            # catch any missing datatypes
            else:
                printLog(f"[WARNING] Encountered undefined datatype for {COLUMN_NAME}: type='{TARGET_DTYPE}', length={DATA_LENGTH}!", LOGFILE)
                printLog("Values must be set manually to ensure compatibility!", LOGFILE)
                TARGET_DTYPE = input("Set target DATA_TYPE: ").strip()
                DATA_LENGTH = input("Set target DATA_LENGTH: ").strip()
                DATA_SCALE = input("Set target DATA_SCALE: ").strip()
                if DATA_LENGTH == '':
                    metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}], '
                elif DATA_SCALE == '':
                    metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}]({DATA_LENGTH}), '
                else:
                    metaString += f'[{COLUMN_NAME}] [{TARGET_DTYPE.lower()}]({DATA_LENGTH},{DATA_SCALE}), '
            

    # strip ', ' from tail
    sourceDTypes = sourceDTypes.rstrip(', ')
    targetColumns = targetColumns.rstrip(', ')
    metaString = metaString.rstrip(', ')
    

    # sqlCreate
    if targetType == 'Oracle':
        sqlCreate = f"CREATE TABLE {schemaName}.{tableName}({metaString})"
    elif targetType == 'MSSQL':
        sqlCreate = f"CREATE TABLE [{schemaName}].[dbo].[{tableName}] ({metaString})"

    return sourceDTypes, targetColumns, sqlCreate

def CustomTypeHandler(rows, sourceDTypes):
    # convert string to list
    sourceDTypes = sourceDTypes.split(',')
    # get indexes of type 'NUMBER'
    ORA_NUMBER_COLS = []
    ORA_DATETIME_COLS = []
    mss_numeric_cols = []
    mss_datetime_cols = []
    for i in range(0, len(sourceDTypes)):
        dataType = sourceDTypes[i].strip()
        # ORACLE
        if dataType == 'NUMBER':
            ORA_NUMBER_COLS.append(i)

        # MS SQL SERVER
        if dataType == 'numeric':
            mss_numeric_cols.append(i)
        if dataType in ['datetime', 'datetime2']:
            mss_datetime_cols.append(i)
    
    # Fix bug DPI-1044: value cannot be represented as an Oracle number
    for rnum in range(0, len(rows)):
        # convert row to list so its values can be mutated
        row = list(rows[rnum])
        # convert Decimal('#.999999...') to float() to avoid DPI-1044
        if len(ORA_NUMBER_COLS) > 0:
            for cnum in ORA_NUMBER_COLS:
                if type(row[cnum]).__name__ == 'Decimal':
                    row[cnum] = float(row[cnum])
        
        if len(mss_numeric_cols) > 0:
            for cnum in mss_numeric_cols:
                if type(row[cnum]).__name__ == 'Decimal':
                    row[cnum] = float(row[cnum])
        
        # convert MSSQL datetime (returned as string) to datetime
        if len(mss_datetime_cols) > 0:
            for cnum in mss_datetime_cols:
                if type(row[cnum]).__name__ == 'str':
                    row[cnum] = parse_datetime(row[cnum])

        # convert row back to tuple
        row = tuple(row)
        # put row back in rows
        rows[rnum] = row
        del row
    
    return rows
