"""
submodule for debug logging to structure script in a better way
"""
import pprint

def debug(row):
    """
    this function prints covid data to stdout
    """
    pp = pprint.PrettyPrinter(indent=1)
    pp.pprint(row)
    print("")

def stdout(msg):
    """
    this function prints msg to stdout
    """
    print(msg)