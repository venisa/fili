#!/usr/local/bin/python

import os

split_tag = os.environ['LAST_TAG'].split(".")
split_tag[-1] = str(int(split_tag[-1]) + 1)
print ".".join(split_tag)