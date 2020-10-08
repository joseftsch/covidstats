"""
submodule for debug logging to structure script in a better way
"""
def print_row(row):
    """
    this function prints covid data to stdout
    """
    print(row["Bezirk"])
    print("AnzEinwohner: ",row["AnzEinwohner"])
    print("Anzahl: ",row["Anzahl"])
    print("AnzahlTot: ",row["AnzahlTot"])
    print("AnzahlFaelle7Tage: ",row["AnzahlFaelle7Tage"])
    print("")

