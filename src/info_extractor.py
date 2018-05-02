# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Capture regex pattern in given text

Author: Pan Deng

"""

import re


def info_extractor(pattern, text, group_num=1, test=False):
    """
    Extract text in line that matches pattern and return the expected extracted text

    :param pattern: the regex pattern to be match
    :param text: the input text
    :param group_num: how many capturing groups are expected
    :param test: whether the function is called for test
    :return: extracted text
    """

    regxObj = re.compile(pattern, re.IGNORECASE | re.UNICODE)
    match = regxObj.search(text)

    if test:
        print('Line: ', text)
        print('Pattern: ', pattern)

    try:
        # Extract the capturing group
        if group_num == 1:
            extract = match.group(1)
        else:
            extract = [match.group(_) for _ in range(1, group_num + 1)]
    # if no matching pattern is found
    except AttributeError:
        extract = ""
    return extract
