import cx_Oracle
import time
from utils import *
from typesHandler import CustomTypeHandler
from sys import exit

def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB or defaultType == cx_Oracle.NCLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize = cursor.arraysize)
    if defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize = cursor.arraysize)


def fetch_metadata(source, tableAddress, excludedList, selectedList):

    # get names
    schemaName = tableAddress.split('.')[0].strip()
    tableName = tableAddress.split('.')[1].strip()

    # build sql condition to exclude/include columns
    columnCondition = ''
    if excludedList != None:
        columnCondition += ' AND COLUMN_NAME NOT IN ('
        for column in excludedList:
            columnCondition += f"'{column.strip()}', "
        # strip ', ' from tail
        columnCondition = columnCondition.rstrip(', ')
        # close paranthesis
        columnCondition += ')'
    if selectedList != None:
        columnCondition += ' AND COLUMN_NAME IN ('
        for column in selectedList:
            columnCondition += f"'{column.strip()}', "
        # strip ', ' from tail
        columnCondition = columnCondition.rstrip(', ')
        # close paranthesis
        columnCondition += ')'

    # build query
    sqlFetchMeta = f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, CHAR_LENGTH
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{schemaName}' AND TABLE_NAME = '{tableName}'
        {columnCondition}
        ORDER BY COLUMN_ID
    """

    # fetch metadata from source db
    meta = None
    while meta == None:
        try:
            sourceConnection = cx_Oracle.connect(user=source.username, password=source.password, dsn=source.dsn, encoding="UTF-8", nencoding="UTF-8")
            with sourceConnection.cursor() as cursor:
                meta = cursor.execute(sqlFetchMeta).fetchall()
        except cx_Oracle.DatabaseError as e:
            printLog(f"ERROR while sqlFetchMeta! {e}", LOGFILE)
            printLog(sqlFetchMeta, LOGFILE)
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


def create_table(source, target, tableAddress, sqlCreate):

    # get names
    schemaName = target.username
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

    sqlDrop = f"DROP TABLE {schemaName}.{tableName}"
    newTblName = None

    targetConnection = cx_Oracle.connect(user=target.username, password=target.password, dsn=target.dsn)
    with targetConnection.cursor() as cursor:
        try:
            cursor.execute(sqlCreate)
            printLog("Target db: new table created.", LOGFILE)
        except cx_Oracle.DatabaseError as e:
            if str(e).find('ORA-00955') != -1:
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
                    except cx_Oracle.DatabaseError as e:
                        printLog(f"[ERROR!!!] {e}", LOGFILE)
                        continue
                elif option == 2:
                    newTblName = input(f"New table name: {schemaName}.").upper()
                    sqlCreate = sqlCreate.replace(tableName, newTblName)
                    cursor.execute(sqlCreate)
                    printLog(f"Target db: table {schemaName}.{newTblName} created.", LOGFILE)
                elif option == 3:
                    exit()
           
    # close connection
    if targetConnection:
        targetConnection.close()

    if newTblName != None:
        targetTableAddr = f"{schemaName}.{newTblName}"
    else:
        targetTableAddr = f"{schemaName}.{tableName}"
    return targetTableAddr


def count_rows(source, tableAddress, filters):
    # open connection
    sourceConnection = cx_Oracle.connect(user=source.username, password=source.password, dsn=source.dsn)

    # get names
    schemaName = tableAddress.split('.')[0].strip()
    tableName = tableAddress.split('.')[1].strip()

    # init sql statement
    sqlCountRows = None
    with sourceConnection.cursor() as cursor:
        if filters == '':
            sqlCountRows = f"SELECT COUNT(rowid) FROM {tableAddress}"
            numRows = cursor.execute(sqlCountRows).fetchone()[0]
        else:
            sqlCountRows = f"SELECT COUNT(rowid) FROM {tableAddress} WHERE {filters}"
            numRows = cursor.execute(sqlCountRows).fetchone()[0]
    # close connection
    if sourceConnection:
        sourceConnection.close()
    return numRows


# fetch batch data
def fetch_batch(batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes):
    
    minrow = 1 + (batch_id - 1) * batchsize
    maxrow = minrow + batchsize - 1

    # reformat column name to match ORACLE standard
    selectedColumnsORACLE = ''
    for column in selectedColumns.split(','):
        selectedColumnsORACLE += f'"{column.strip()}", '
    selectedColumnsORACLE = selectedColumnsORACLE.rstrip(', ')

    if filters != '':
        filters = 'AND ' + filters
    sqlSelect = f"""
        SELECT {selectedColumnsORACLE}
        FROM {tableAddress} A,
        (
            SELECT RID, rownum rnum
            FROM
            (
                SELECT /*+ first_rows(100) */ rowid as RID
                FROM {tableAddress}
                WHERE 1=1
                {filters}
                ORDER BY rowid
            )
            WHERE rownum <= {maxrow}
        )
        WHERE rnum >= {minrow} and RID = a.rowid
    """

    rows = None
    encoding = True
    while rows == None:
        try:
            if encoding == True:
                sourceConnection = cx_Oracle.connect(user=source.username, password=source.password, dsn=source.dsn, encoding="UTF-8", nencoding="UTF-8")
            else:
                sourceConnection = cx_Oracle.connect(user=source.username, password=source.password, dsn=source.dsn)
            
            sourceConnection.outputtypehandler = OutputTypeHandler

            with sourceConnection.cursor() as cursor:
                printLog(f'batch {batch_id}: started', LOGFILE)
                rows = cursor.execute(sqlSelect).fetchall()
        except cx_Oracle.DatabaseError as e:
            printLog(f"ERROR while sqlSelect! {e}", LOGFILE)
            if str(e).find('ORA-02391') != -1:
                pass
            else:
                printLog(sqlSelect, LOGFILE)
            printLog(f'batch {batch_id}: retrying after 10s...', LOGFILE)
            time.sleep(10)
        except UnicodeDecodeError as e:
            printLog(f"ERROR while sqlSelect! {e}", LOGFILE)
            printLog(sqlSelect, LOGFILE)
            printLog(f'batch {batch_id}: retrying without encoding...', LOGFILE)
            encoding = False
            rows = None

    if sourceConnection:
        sourceConnection.close()

    # Handle custom datatypes
    rows = CustomTypeHandler(rows, sourceDTypes)
    
    printLog(f'batch {batch_id}: fetched', LOGFILE)

    return rows, encoding

# push batch data
def push_batch(batch_id, rows, encoding, target, targetTableAddr, targetColumns):

    valueString = ''
    for i in range(1, len(rows[0]) + 1):
        valueString += f":{i}, "
    valueString = valueString.rstrip(', ')

    # make sure column names match Oracle standard
    # selectedColumns = return_unsigned(selectedColumns).upper()

    sqlInsert = f"INSERT INTO {targetTableAddr}({targetColumns}) values ({valueString})"
    inserted = False
    while inserted == False:
        try:
            if encoding == True:
                targetConnection = cx_Oracle.connect(user=target.username, password=target.password, dsn=target.dsn, encoding="UTF-8", nencoding="UTF-8")
            else:
                targetConnection = cx_Oracle.connect(user=target.username, password=target.password, dsn=target.dsn)
            with targetConnection.cursor() as cursor:
                cursor.executemany(sqlInsert, rows)
            inserted = True
        except cx_Oracle.DatabaseError as e:
            if str(e).find('ORA-01861') != -1:
                printLog("[ERROR!!!] Inserted values did not match target datatypes!", LOGFILE)
                printLog("Correct sqlCreate statement in 'profiles.ini' or create new ETL profile!", LOGFILE)
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