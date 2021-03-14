import configparser
from TableIt import printTable
from utils import *
import libOracle
import libMSSQL
from typesHandler import *
from sys import exit
import codecs

def get_column_lists(selectedColumns, excludedColumns):
    # excludedColumns
    excludedList = None
    if excludedColumns.strip() != '':
        excludedList = []
        for column in excludedColumns.split(','):
            excludedList.append(column.strip())
    
    # selectedColumns
    selectedList = None
    if selectedColumns.strip() != '':
        selectedList = []
        for column in selectedColumns.split(','):
            if excludedList != None:
                if column.strip() not in excludedList:
                    selectedList.append(column.strip())
            else:
                selectedList.append(column.strip())

    # confirm messages
    if selectedList != None:    
        for column in selectedList:
            printLog(f"{column.strip()} included!", LOGFILE)
    else:
        printLog(f"All columns included!", LOGFILE)

    if excludedList != None:    
        for column in excludedList:
            printLog(f"{column.strip()} excluded!", LOGFILE)
    else:
        printLog(f"No column excluded!", LOGFILE)

    return excludedList, selectedList

def get_user_inputs():
    # Get table name
    tableAddress = input("\nTable name (OWNER.TABLE_NAME): ").strip().upper()
    while len(tableAddress.split('.')) != 2:
        print("Wrong name format! Correct format: OWNER.TABLE_NAME")
        tableAddress = input("\nRe-enter table name: ").strip().upper()

    # Get column names
    selectedColumns = input("\nColumn names separated by , (Default=*|all columns): ").replace('*', '').strip().upper()

    # Select columns to be excluded
    excludedColumns = input("\nColumns to be excluded?: ").strip().upper()

    # Filter conditions
    filters = input("\nAdd filters: WHERE ").strip().upper()

    # Set batch size (number of rows per batch)
    #batchsize = None
    #while True:
    #    batchsize = input("\nSet batch size (Default=100,000): ").replace(',','').strip()
    #    if batchsize == '':
    #        batchsize = '100000'
    #    if batchsize.isnumeric() and int(batchsize) >= 1000:
    #        break
    #    else:
    #        print("Batch size must be a number >= 1000!")
    batchsize = '100000'  

        
    # Limit number of rows to get
    rowLimit = input("Set row limit (Default=0|no limit): ").replace(',','').strip() # 0 means no limit
    if rowLimit == '':
        rowLimit = '0'

    print("-------------------------------------------------------------")

    # get column lists from user inputs
    excludedList, selectedList = get_column_lists(selectedColumns, excludedColumns)

    return tableAddress, excludedList, selectedList, filters, batchsize, rowLimit


def create_new_profile(source, target, config):
    sourceType = type(source).__name__
    targetType = type(target).__name__
    mapTitle = f"{sourceType} --> {targetType}"
    
    tableAddress, excludedList, selectedList, filters, batchsize, rowLimit = get_user_inputs()

    # fetch metadata, must include excludedList in function inputs in case user left selectedColumns blank.
    meta = None
    if sourceType == 'Oracle':
        meta, selectedList = libOracle.fetch_metadata(source, tableAddress, excludedList, selectedList)
    elif sourceType == 'MSSQL':
        meta, selectedList = libMSSQL.fetch_metadata(source, tableAddress, excludedList, selectedList)

    mappingDict = None
    if sourceType != targetType:
        mappingDict = get_mappingDict(mapTitle)

    sourceDTypes, targetColumns, sqlCreate = sqlCreateBuilder(source, target, tableAddress, meta, mappingDict)
    
    # convert selectedList to string to save
    selectedColumns = ''
    for columnName in selectedList:
        selectedColumns += f"{columnName}, "
    selectedColumns = selectedColumns.rstrip(', ')

    # create job profile and write to file
    profileName = f"{tableAddress}: {mapTitle}"
    config[profileName] = {}
    profileObject = config[profileName]
    profileObject['tableAddress'] = tableAddress
    profileObject['selectedColumns'] = selectedColumns
    profileObject['filters'] = filters
    profileObject['batchsize'] = batchsize
    profileObject['rowLimit'] = rowLimit
    profileObject['sourceDTypes'] = sourceDTypes
    profileObject['targetColumns'] = targetColumns
    profileObject['sqlCreate'] = sqlCreate
    
    # save to file
    with codecs.open('profiles.ini', 'w+', 'utf8') as configfile:
        config.write(configfile)

    return tableAddress, selectedColumns, filters, batchsize, rowLimit, sourceDTypes, targetColumns, sqlCreate


# View profiles
def view_profiles(config):
    i = 0
    profilesTable = [['id', 'name']]
    for profileName in config.sections():
        i += 1
        profilesTable.append([f'{i}', profileName])
        
    numSaved = len(profilesTable) - 1
    print(f"Found {numSaved} saved job profiles.")
    if numSaved > 0:
        printTable(profilesTable, useFieldNames=True)

    return None


# Profile picker
def profile_picker(config):
    view_profiles(config)
    keyMap = dict()
    i = 0
    for key in config.sections():
        i += 1
        keyMap[i] = key

    option = None
    profileName = None
    profileObject = None
    while option not in keyMap.keys():
        option = input("Profile id: ")
        if option.isnumeric():
            option = int(option)

    profileName = keyMap.get(option, None)
    profileObject = config[profileName]

    print("-------------------------------------------------------------")
    for key in profileObject:
        value = profileObject[key]
        if len(value) > 128:
            value = f"{value[0:100]}..."
        print(f"{key} = {value}")
    print("-------------------------------------------------------------")

    return profileName, profileObject


def select_profile(config):
    printViewTitle('SELECT JOB PROFILE')
    confirmed = False
    while confirmed == False:
        profileName, profileObject = profile_picker(config)
        textInput = None
        while textInput not in ['Y', 'N', '']:
            textInput = input("Is this the profile you want? (Y/n): ").strip().upper()
            confirmed = (textInput in ['Y', ''])
            if textInput == 'N':
                option = None
                print("Ok, select a different one to continue...")
    tableAddress = profileObject['tableAddress']
    selectedColumns = profileObject['selectedColumns']
    filters = profileObject['filters']
    batchsize = profileObject['batchsize']
    rowLimit = profileObject['rowLimit']
    sourceDTypes = profileObject['sourceDTypes']
    targetColumns = profileObject['targetColumns']
    sqlCreate = profileObject['sqlCreate']

    return tableAddress, selectedColumns, filters, batchsize, rowLimit, sourceDTypes, targetColumns, sqlCreate
    

# Delete a profile
def delete_profile(config):
    printViewTitle('DELETE JOB PROFILE')
    profileName, profileObject = profile_picker(config)
    print(f"{profileName} is going to be deleted!")
    textInput = ''
    confirmed = False
    while textInput.upper() not in ['YES', 'NO']:
        textInput = input("Are you sure? (yes/no): ")
        confirmed = (textInput.upper() == 'YES')
    if confirmed:
        config.remove_section(profileName)
        print(f"{profileName} deleted!")
    else:
        # go back to manage_profiles loop
        pass
    # save to file
    with codecs.open('profiles.ini', 'w+', 'utf8') as configfile:
        config.write(configfile)

    return config


# Manage profiles
def manage_profiles(source, target):

    config = configparser.ConfigParser()
    # create file if not existed
    with open('profiles.ini', 'a+') as configfile:
        pass
    # read from file
    with codecs.open('profiles.ini', 'r', 'utf8') as configfile:
        config.read_file(configfile)
    
    tableAddress = None
    selectedColumns = None
    filters = None
    batchsize = None
    rowLimit = None
    sourceDTypes = None
    targetColumns = None
    sqlCreate = None
    

    option = None
    while True:
        printViewTitle("SELECT OR CREATE JOB PROFILE")
        numSaved = len(config.sections())
        if option != 2:
            if numSaved > 0:
                print("What would you like to do?\n (1) Select profile\n (2) Create new profile\n (3) Delete profile\n (4) Exit")
                
                while option not in range(1,5):
                    print("You must select a profile or create one to continue!")
                    option = input("Enter an option number: ")
                    if option.isnumeric():
                        option = int(option)
            else:
                # if 0 saved profiles, jump directly to create one
                print("No saved profile. Must create one to continue!")
                option = 2
        else:
            # if profile created previously, jump directly to select one
            print("Select a profile to continue!")
            option = 1

        if option == 1:
            tableAddress, selectedColumns, filters, batchsize, rowLimit, sourceDTypes, targetColumns, sqlCreate = select_profile(config)
            # break the loop if a profile is selected
            break
        elif option == 2:
            tableAddress, selectedColumns, filters, batchsize, rowLimit, sourceDTypes, targetColumns, sqlCreate = create_new_profile(source, target, config)
        elif option == 3:
            config = delete_profile(config)
            # after deletion, reset option
            option = None
        elif option == 4:
            exit()
        


    # convert batchsize & rowLimit to int
    batchsize = int(batchsize)
    rowLimit = int(rowLimit)
    return tableAddress, selectedColumns, filters, batchsize, rowLimit, sourceDTypes, targetColumns, sqlCreate
