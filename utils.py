from datetime import datetime
import os, os.path
import errno


# GLOBAL VARIABLES
VERSION = "v.2020.0511.1"
LOGFILE = str(datetime.now()).replace('-', '').replace(' ', '_').replace(':', '').replace('.', '_') + '.log'
# print(LOGFILE)


# Taken from https://stackoverflow.com/a/600612/119527
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def safe_open(path, permission):
    ''' Open "path" with "permission", creating any parent directories as needed.
    '''
    mkdir_p(os.path.dirname(path))
    return open(path, permission)

# My custom util functions
def printLog(textLine, filename):
    print(textLine)
    path = os.path.join(os.getcwd(), 'Logs', filename)
    with safe_open(path, 'a+') as logFile:
        logFile.write(f'{textLine}\n')
    return None

def get_new_input(currentValue, hint):
    textInput = input(hint)
    if textInput != '':
        return textInput
    else:
        return currentValue

def printViewTitle(text):
    lineBreaker = '+-----------------------------------------------------------+'
    lSpaces = int((len(lineBreaker) - len(text) - 2) / 2)
    rSpaces = len(lineBreaker) - lSpaces - len(text) - 2
    print(lineBreaker)
    print('|' + lSpaces*' ' + text + rSpaces *' ' + '|')
    print(lineBreaker)
    return None

def time_converter(timeSec):
    if timeSec >= 60*60*24:
        timeTxt = str(round(timeSec/60/60/24, 1)) + ' days'
    elif timeSec >= 60*60:
        timeTxt = str(round(timeSec/60/60, 1)) + ' hours'
    elif timeSec >= 60:
        timeTxt = str(round(timeSec/60, 1)) + ' minutes'
    else:
        timeTxt = str(round(timeSec, 1)) + ' seconds'
    return timeTxt

def return_int(textString):
    if textString.isnumeric():
        output = int(textString)
    else:
        output = 0
    return output

# custom datetime parsing function that reduces CPU cost significantly
def parse_datetime(datetime_str):
    # 2017-09-28 00:00:00.0000000
    year = return_int(datetime_str[0:4])
    month = return_int(datetime_str[5:7])
    day = return_int(datetime_str[8:10])
    hour = return_int(datetime_str[11:13])
    minute = return_int(datetime_str[14:16])
    second = return_int(datetime_str[17:19])
    fraction = return_int(datetime_str[20:26])
    
    return datetime(year, month, day, hour, minute, second, fraction)


# Handle Vietnamese column names
import string
import re

def return_unsigned(utf8_str):
    INTAB = "ạảãàáâậầấẩẫăắằặẳẵẠẢÃÀÁÂẬẦẤẨẪĂẮẰẶẲẴóòọõỏôộổỗồốơờớợởỡÓÒỌÕỎÔỘỔỖỒỐƠỜỚỢỞỠéèẻẹẽêếềệểễÉÈẺẸẼÊẾỀỆỂỄúùụủũưựữửừứÚÙỤỦŨƯỰỮỬỪỨíìịỉĩÍÌỊỈĨýỳỷỵỹÝỲỶỴỸđĐ "

    # INTAB = [ch.encode('utf8') for ch in unicode(INTAB, 'utf8')]

    OUTTAB = "a"*17 + "A"*17 + "o"*17 + "O"*17 + "e"*11 + "E"*11 + "u"*11 + "U"*11 + "i"*5  + "I"*5 + "y"*5 + "Y"*5 + "d" + "D" + "_"

    r = re.compile("|".join(INTAB))
    replaces_dict = dict(zip(INTAB, OUTTAB))

    return r.sub(lambda m: replaces_dict[m.group(0)], utf8_str)

import textwrap

def text_wrapper(width, text_input):
    wrapper = textwrap.TextWrapper(width=width)
    word_list = wrapper.wrap(text=str(text_input))
    result = ''
    for el in word_list:
        result += f'{el}\n'
    return result