txt = '------------------------------------------'
print(len(txt))

futs = {'task 1': 1, 'task 2' : 2}
print(list(futs.values()))

import textwrap

def text_wrapper(width, text_input):
    wrapper = textwrap.TextWrapper(width=width)
    word_list = wrapper.wrap(text=str(text_input))
    result = ''
    for el in word_list:
        result += f'{el}\n'
    return result


active_workers = [value + 1 for value in list(futs.values())]
print(active_workers)