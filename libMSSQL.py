import pyodbc 
from utils import *
import time
from typesHandler import CustomTypeHandler
from sys import exit


def fetch_metadata(source, tableAddress, excludedList, selectedList):

    # get names
    schemaName = tableAddress.split('.')[0].strip()
    tableName = tableAddress.split('.')[1].strip()

    # build sql condition to exclude/include columns
    columnCondition = ''
    if excludedList != None:
        columnCondition += ' AND COL.NAME NOT IN ('
        for column in excludedList:
            columnCondition += f"'{column.strip()}', "
        # strip ', ' from tail
        columnCondition = columnCondition.rstrip(', ')
        # close paranthesis
        columnCondition += ')'
    if selectedList != None:
        columnCondition += ' AND COL.NAME IN ('
        for column in selectedList:
            columnCondition += f"'{column.strip()}', "
        # strip ', ' from tail
        columnCondition = columnCondition.rstrip(', ')
        # close paranthesis
        columnCondition += ')'

    # build query
    sqlFetchMeta = f"""
        SELECT COLUMN_NAME, DATA_TYPE,
        CHARACTER_OCTET_LENGTH AS DATA_LENGTH,
        NUMERIC_PRECISION AS DATA_PRECISION,
        NUMERIC_SCALE AS DATA_SCALE,
        IS_NULLABLE AS NULLABLE,
        CHARACTER_MAXIMUM_LENGTH AS CHAR_LENGTH
        FROM {source.database}.INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
        AND TABLE_SCHEMA = '{schemaName}'
        AND TABLE_NAME = '{tableName}'
        {columnCondition}
        ORDER BY ORDINAL_POSITION
    """

    # fetch metadata from source db
    meta = None
    while meta == None:
        try:
            sourceConnection = pyodbc.connect('Driver={'+source.driver+'};'
                                              'Server='+source.server+';'
                                              'Database='+source.database+';'
                                              'UID='+source.username+';'
                                              'password='+source.password+';'
                                              'Trusted_Connection=yes;')
            with sourceConnection.cursor() as cursor:
                meta = cursor.execute(sqlFetchMeta).fetchall()
        except pyodbc.Error as e:
            printLog(f"ERROR while sqlFetchMeta! {e}", LOGFILE)
            printLog(sqlFetchMeta, LOGFILE)
            if str(e).find('Invalid column name') != -1:
                exit()
            printLog("Retrying to fetch metdata...", LOGFILE)

    # if user left selectedColumns as blank/*, fetch selectedList from source
    if selectedList == None:
        selectedList = []
        for row in meta:
            selectedList.append(row[0])

    # close connection
    if sourceConnection:
        sourceConnection.close()
    # numCols
    numCols = len(meta)
    printLog(f'Source db: Metadata fetched for {numCols} columns.', LOGFILE)

    return meta, selectedList


def count_rows(source, tableAddress, filters):
    # open connection
    sourceConnection = pyodbc.connect('Driver={'+source.driver+'};'
                                      'Server='+source.server+';'
                                      'Database='+source.database+';'
                                      'UID='+source.username+';'
                                      'password='+source.password+';'
                                      'Trusted_Connection=yes;')

    # get names
    schemaName = tableAddress.split('.')[0].strip()
    tableName = tableAddress.split('.')[1].strip()

    # init sql statement
    sqlCountRows = None
    with sourceConnection.cursor() as cursor:
        if filters == '':
            sqlCountRows = f"SELECT COUNT(1) FROM {tableAddress}"
            numRows = cursor.execute(sqlCountRows).fetchone()[0]
        else:
            sqlCountRows = f"SELECT COUNT(1) FROM {tableAddress} WHERE {filters}"
            numRows = cursor.execute(sqlCountRows).fetchone()[0]
    # close connection
    if sourceConnection:
        sourceConnection.close()
    return numRows


# fetch batch data
def fetch_batch(batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes):
    
    minrow = 1 + (batch_id - 1) * batchsize
    # maxrow = minrow + batchsize - 1

    # get names
    schemaName = tableAddress.split('.')[0].strip()
    tableName = tableAddress.split('.')[1].strip()

    # reformat column name to match MSSQL standard
    selectedColumnsMSSQL = ''
    for column in selectedColumns.split(','):
        selectedColumnsMSSQL += f"[{column.strip()}], "
    selectedColumnsMSSQL = selectedColumnsMSSQL.rstrip(', ')

    if filters != '':
        filters = 'AND ' + filters
    sqlSelect = f"""
        DECLARE @Index int = {minrow}
        DECLARE @Count int = {batchsize}

        SELECT {selectedColumnsMSSQL} FROM
        (
            SELECT TOP (@Index + @Count) *,
                ROW_NUMBER() over (order by %%physloc%%) as RNUM
            FROM {source.database}.{schemaName}.{tableName}
        ) as T
        WHERE RNUM BETWEEN @Index and @Index + @Count - 1
        {filters}
    """

    rows = None
    encoding = True
    while rows == None:
        try:
            sourceConnection = pyodbc.connect('Driver={'+source.driver+'};'
                                              'Server='+source.server+';'
                                              'Database='+source.database+';'
                                              'UID='+source.username+';'
                                              'password='+source.password+';'
                                              'Trusted_Connection=yes;')
            with sourceConnection.cursor() as cursor:
                printLog(f'batch {batch_id}: started', LOGFILE)
                rows = cursor.execute(sqlSelect).fetchall()
        except pyodbc.Error as e:
            printLog(f"ERROR while sqlSelect! {e}", LOGFILE)
            printLog(sqlSelect, LOGFILE)
            printLog(f'batch {batch_id}: retrying after 10s...', LOGFILE)
            time.sleep(10)
        # except UnicodeDecodeError as e:
        #     printLog(f"ERROR while sqlSelect! {e}", LOGFILE)
        #     printLog(sqlSelect, LOGFILE)
        #     printLog(f'batch {batch_id}: retrying without encoding...', LOGFILE)
        #     encoding = False
        #     rows = None

    if sourceConnection:
        sourceConnection.close()

    # Handle custom datatypes
    rows = CustomTypeHandler(rows, sourceDTypes)
    
    printLog(f'batch {batch_id}: fetched', LOGFILE)

    return rows, encoding


def create_table(source, target, tableAddress, sqlCreate):
    # get names
    schemaName = target.database
    tableName = tableAddress.split('.')[1].strip()

    # allow user to change schema name if they have privilege to do so
    # isAnotherSchema = None
    # while isAnotherSchema not in ['', 'Y' 'N']:
    #     isAnotherSchema = input(f"Insert into a different schema from your user-schema '{schemaName}'? (y/N)").strip().upper()
    #     if isAnotherSchema == 'Y':
    #         schemaName = input("Specify a target schema: ").strip().upper()
    #         printLog(f"New schema name: {schemaName}.", LOGFILE)
    #         sqlCreate = sqlCreate.replace(f"{target.username}.{tableName}", f"{schemaName}.{tableName}")
    #     elif isAnotherSchema in ['', 'N']:
    #         printLog(f"Using target username '{schemaName}' as schema.", LOGFILE)

    sqlDrop = f"DROP TABLE {schemaName}.dbo.{tableName}"
    newTblName = None

    targetConnection = pyodbc.connect('Driver={'+target.driver+'};'
                                      'Server='+target.server+';'
                                      'Database='+target.database+';'
                                      'UID='+target.username+';'
                                      'password='+target.password+';'
                                      'Trusted_Connection=yes;')
    with targetConnection.cursor() as cursor:
        try:
            cursor.execute(sqlCreate)
            printLog("Target db: new table created.", LOGFILE)
        except pyodbc.Error as e:
            if str(e).find('already an object') != -1:
                printLog("Found existing table with identical name!", LOGFILE)
            else:
                printLog(f"ERROR while sqlCreate! {e}", LOGFILE)
                printLog(sqlCreate, LOGFILE)
            option = None
            while option not in [1, 2, 3]:
                option = input("What do you want to do?\n (1) Drop existing table\n (2) New table name\n (3) Exit\n Enter a number: ")
                if option.isnumeric():
                    option = int(option)
                if option == 1:
                    try:
                        cursor.execute(sqlDrop)
                        printLog("Target db: existing table dropped.", LOGFILE)
                        cursor.execute(sqlCreate)
                        printLog("Target db: new table created.", LOGFILE)
                        break
                    except pyodbc.Error as e:
                        printLog(f"[ERROR!!!] {e}", LOGFILE)
                        continue
                elif option == 2:
                    newTblName = input(f"New table name: {schemaName}.dbo.").upper()
                    sqlCreate = sqlCreate.replace(tableName, newTblName)
                    cursor.execute(sqlCreate)
                    printLog(f"Target db: table {schemaName}.dbo.{newTblName} created.", LOGFILE)
                elif option == 3:
                    exit()
           
    # close connection
    if targetConnection:
        targetConnection.close()

    if newTblName != None:
        targetTableAddr = f"{schemaName}.dbo.{newTblName}"
    else:
        targetTableAddr = f"{schemaName}.dbo.{tableName}"
    return targetTableAddr

# push batch data
def push_batch(batch_id, rows, encoding, target, targetTableAddr, targetColumns):
    
    valueString = ''
    for i in range(1, len(rows[0]) + 1):
        valueString += "?, "
    valueString = valueString.rstrip(', ')

    sqlInsert = f"INSERT INTO {targetTableAddr} ({targetColumns}) VALUES ({valueString})"
    inserted = False
    while inserted == False:
        try:
            targetConnection = pyodbc.connect('Driver={'+target.driver+'};'
                                              'Server='+target.server+';'
                                              'Database='+target.database+';'
                                              'UID='+target.username+';'
                                              'password='+target.password+';'
                                              'Trusted_Connection=yes;')
            with targetConnection.cursor() as cursor:
                # fast_executemany: can boost the performance of executemany operations by greatly reducing the number of round-trips to the server.
                # Only recommended for applications that use Microsoft's ODBC Driver for SQL Server.
                cursor.fast_executemany = True
                cursor.executemany(sqlInsert, rows)
            inserted = True
        except pyodbc.Error as e:
            if str(e).find('overflow') != -1:
                printLog("[ERROR!!!] Data overflow!", LOGFILE)
                printLog("Dumping the first 5 rows of this batch:", LOGFILE)
                printLog(f"{rows[1]}\n{rows[2]}\n{rows[3]}\n{rows[4]}\n{rows[5]}", LOGFILE)
                exit()
            else:
                printLog(f"ERROR while sqlInsert! {e}", LOGFILE)
                printLog(sqlInsert, LOGFILE)
            printLog(f'batch {batch_id}: retrying after 30s...', LOGFILE)
            time.sleep(30)
            inserted = False
        
    targetConnection.commit()
    # goodbye rows
    del rows

    printLog(f'batch {batch_id}: commited', LOGFILE)
    
    # close target connection
    if targetConnection:
        targetConnection.close()
    
    return time.time()