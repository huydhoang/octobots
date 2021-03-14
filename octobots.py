import os.path
import cx_Oracle
from collections import namedtuple
# import decimal # import for type conversion
# dummy = decimal.Decimal('1') #get rid of 'imported but unused' warning

import time
import scheduler

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from itertools import repeat
import concurrent.futures
import itertools

import psutil
from getfootprint import *

import gc

from connections import *
from utils import *
from datetime import datetime

import libOracle
import libMSSQL

from jobProfiles import *

from crashReport import *


def copyright():
    printViewTitle(f"OCTOBOTS {VERSION}")
    print("Developed by @huydhoang https://github.com/huydhoang\n")

    return None


def gen_batches(source, tableAddress, filters, rowLimit, batchsize):
    sourceType = type(source).__name__

    # count rows
    if sourceType == 'Oracle':
        numRows = libOracle.count_rows(source, tableAddress, filters)
    elif sourceType == 'MSSQL':
        numRows = libMSSQL.count_rows(source, tableAddress, filters)

    printLog(f'Number of rows: {numRows}', LOGFILE)
    if rowLimit > 0 and rowLimit < numRows:
        numRows = rowLimit
        printLog(f"LIMIT: {rowLimit}", LOGFILE)

    # round up for number of batches
    numBatches = int(numRows / batchsize) + (numRows % batchsize > 0)
    printLog(f'Number of batches: {numBatches}', LOGFILE)

    batchList = []
    for i in range(1,numBatches+1):
        batchList.append(i)
    return batchList, numRows


def mainPipe(batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes, target, targetTableAddr, targetColumns):
    sourceType = type(source).__name__
    targetType = type(target).__name__

    # fetch batch data
    if sourceType == 'Oracle':
        rows, encoding = libOracle.fetch_batch(batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes)
    elif sourceType == 'MSSQL':
        rows, encoding = libMSSQL.fetch_batch(batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes)

    # push batch data
    if targetType == 'Oracle':
        finishTime = libOracle.push_batch(batch_id, rows, encoding, target, targetTableAddr, targetColumns)
    elif targetType == 'MSSQL':
        finishTime = libMSSQL.push_batch(batch_id, rows, encoding, target, targetTableAddr, targetColumns)

    return finishTime

def get_max_workers(batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes):
    printLog("------------------------------------------", LOGFILE)
    # get free mem in bytes
    freeBytes = psutil.virtual_memory().free
    for i in range(0,3):
        print("\rGetting available memory" + '.'*(i%4) + ' '*(3 - i%4), end='')
        time.sleep(1)
        freeBytes = (freeBytes + psutil.virtual_memory().free) / 2
    printLog(f'\nAvailable memory: {round(freeBytes/1024/1024)}MB', LOGFILE)

    # get 1st batch
    printLog('Fetching batch data...', LOGFILE)
    sourceType = type(source).__name__
    if sourceType == 'Oracle':
        rows = libOracle.fetch_batch(1, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes)
    elif sourceType == 'MSSQL':
        rows = libMSSQL.fetch_batch(1, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes)

    unpacked_rows = [str(row) for row in rows]

    # get memory footprint for processing a batch
    batchBytes = total_size(rows)
    batchRAMUs = batchBytes + total_size(unpacked_rows)*5
 
    # get rid of batch data
    del rows
    del unpacked_rows
    
    printLog(f'Loaded batch size: {round(batchBytes/1024/1024)}MB', LOGFILE)
    printLog(f'Est. RAM usage/batch: {round(batchRAMUs/1024/1024)}MB', LOGFILE)
 
    # provision 1GB for doing other works
    max_workers = int((freeBytes - 1024*1024*1024)/batchRAMUs)
    # max_workers = 16

    # max_workers cannot be less than 1
    if max_workers <= 0:
        max_workers = 1
    # for memory safety, max_workers cannot be more than 300
    elif max_workers > 300:
        max_workers = 300

    printLog(f'Set max_workers={max_workers}', LOGFILE)
    printLog("------------------------------------------", LOGFILE)

    return max_workers



if __name__ == '__main__':
    
    copyright()

    try:

        # select source and target
        source, target, fileWriteErr = select_source_target()
        if fileWriteErr != None:
            printLog(fileWriteErr, LOGFILE)

        # get ETL job parameters from profile
        tableAddress, selectedColumns, filters, batchsize, rowLimit, sourceDTypes, targetColumns, sqlCreate = manage_profiles(source, target)
        numCols = len(selectedColumns.split(','))
        printLog(f"Source table: {tableAddress}", LOGFILE)

        # set job schedule
        job_schedule = scheduler.set_schedule()

        # create table in the target db
        targetType = type(target).__name__
        if targetType == 'Oracle':
            targetTableAddr = libOracle.create_table(source, target, tableAddress, sqlCreate)
        elif targetType == 'MSSQL':
            targetTableAddr = libMSSQL.create_table(source, target, tableAddress, sqlCreate)

        # batchsize and max_workers
        isAdaptive = None
        while isAdaptive not in ['Y', 'N', '']:
            isAdaptive = input("Let Octobots decide batchsize and max_workers? (Y/n): ").strip().upper()
        
        if isAdaptive in ['Y', '']:
            # get max workers
            max_workers = get_max_workers(batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes)
        else:
            # let user manually set the params
            batchsize = input("batchsize: ").strip().replace(',', '')
            while batchsize.isnumeric() == False:
                batchsize = input("batchsize: ").strip().replace(',', '')
            batchsize = int(batchsize)
            print(f"batchsize={batchsize}")
            
            max_workers = input("max_workers: ").strip().replace(',', '')
            while max_workers.isnumeric() == False:
                max_workers = input("max_workers: ").strip().replace(',', '')
            max_workers = int(max_workers)
            print(f"max_workers={max_workers}")

        # generate batch list for batch select/insert
        batchList, numRows = gen_batches(source, tableAddress, filters, rowLimit, batchsize)

        # start counting down until job schedule
        if job_schedule != None:
            scheduler.sleep_until(job_schedule)

        # start the timer
        start = time.time()
        
        # parallel processing
        iterator = iter(batchList)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.
            futures = {
                executor.submit(mainPipe, batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes, target, targetTableAddr, targetColumns): batch_id
                for batch_id in itertools.islice(iterator, max_workers)
            }

            cntCur = 0
            commited_batches = []
            while futures:
                # Wait for the next future to complete.
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    original_task = futures.pop(fut)
                    commited_batches.append(original_task)
                    txt_commited = f'Commited batches: {len(commited_batches)}\n{text_wrapper(42, commited_batches)}'
                    printLog(f'{txt_commited}', LOGFILE)

                    # print(f"Number {original_task} done in {round(time.time() - fut.result(), 1)} seconds.")
                    # force the Garbage Collector to release unreferenced memory
                    gc.collect()
                    # print batch finish messages
                    cntCur += 1
                    cntRem = len(batchList) - cntCur
                    avgBat = (fut.result() - start) / cntCur
                    timeRemSec = cntRem * avgBat
                    timeRemTxt = time_converter(timeRemSec)
                    line_breaker = '------------------------------------------'
                    txt_progress = f'Progress: {cntCur}/{len(batchList)}'
                    txt_time_rmn = f'Exp. time remaining: {timeRemTxt}'
                    txt_time_fin = f'Exp. finish time: {time.ctime(time.time() + timeRemSec)}'
                    txt_to_print = f'{line_breaker}\n{txt_progress}\n{txt_time_rmn}\n{txt_time_fin}\n{line_breaker}\n'
                    printLog(txt_to_print, LOGFILE)

                # Schedule the next set of futures.  We don't want more than N futures
                # in the pool at a time, to keep memory consumption down.
                for batch_id in itertools.islice(iterator, len(done)):
                    fut = executor.submit(mainPipe, batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes, target, targetTableAddr, targetColumns)
                    futures[fut] = batch_id
                    printLog(f"Active workers: {len(futures)}", LOGFILE)
                    active_workers = list(futures.values())
                    printLog(f"{text_wrapper(42, active_workers)}", LOGFILE)


        # with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # futures = [executor.submit(mainPipe, batch_id, batchsize, source, tableAddress, selectedColumns, filters, sourceDTypes, target, targetTableAddr, targetColumns) for batch_id in batchList]

            # cntCur = 0
            # for future in as_completed(futures):
            #     # force the Garbage Collector to release unreferenced memory
            #     gc.collect()
            #     # print batch finish messages
            #     cntCur += 1
            #     cntRem = len(batchList) - cntCur
            #     avgBat = (future.result() - start) / cntCur
            #     timeRemSec = cntRem * avgBat
            #     timeRemTxt = time_converter(timeRemSec)
            #     line_breaker = '------------------------------------------'
            #     txt_progress = f'Progress: {cntCur}/{len(batchList)}'
            #     txt_time_rmn = f'Exp. time remaining: {timeRemTxt}'
            #     txt_time_fin = f'Exp. finish time: {time.ctime(time.time() + timeRemSec)}'
            #     txt_to_print = f'{line_breaker}\n{txt_progress}\n{txt_time_rmn}\n{txt_time_fin}\n{line_breaker}'
            #     printLog(txt_to_print, LOGFILE)
        
        # conclude job benchmark
        timeTtlSec = time.time() - start
        timeTtlTxt = time_converter(timeTtlSec)
        printLog(f'Finished in {timeTtlTxt}.', LOGFILE)
        # numRows = rowLimit if rowLimit > 0 and < numRows
        printLog(f'Time per 1M rows x 10 cols: {round(timeTtlSec / (numCols * numRows) * 10e6, 1)} seconds.', LOGFILE)

        printLog(f'\nEnd of {LOGFILE}', LOGFILE)

    except:
        crashreport()
        raise

    # keep the window open until user press Enter
    input("\nPress Enter to close...")