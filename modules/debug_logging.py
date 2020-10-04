"""
submodule for debug logging to structure script in a better way
"""
def print_row(row):
    """
    this function prints covid data to stdout
    """
    print(row["Bezirk"],":",row["Anzahl"],":",row["Timestamp"])
