import tables
from tables import *

class fnotable(IsDescription):
    TIMESTAMP = Time64Col()
    INSTRUMENT = StringCol(6)
    SYMBOL = StringCol(32)
    EXPIRY_DT = Time64Col()
    STRIKE_PR = Float64Col()
    OPTION_TYP = StringCol(6)
    OPEN = Float64Col()
    HIGH = Float64Col()
    LOW = Float64Col()
    CLOSE = Float64Col()
    SETTLE_PR = Float64Col()
    CONTRACTS = Float64Col()
    VAL_INLAKH = Float64Col()
    OPEN_INT = Float64Col()
    CHG_IN_OI = Float64Col()

class indextable(IsDescription):
    TIMESTAMP = Time64Col()
    SYMBOL = StringCol(32)
    OPEN = Float64Col()
    HIGH = Float64Col()
    LOW = Float64Col()
    CLOSE = Float64Col()
    TOTTRDQTY = Float64Col()
    TOTTRDVAL = Float64Col()

class stocktable(IsDescription):
    SYMBOL = StringCol(32)
    SERIES = StringCol(4)
    OPEN = Float64Col()
    HIGH = Float64Col()
    LOW = Float64Col()
    CLOSE = Float64Col()
    LAST = Float64Col()
    PREVCLOSE = Float64Col()
    TOTTRDQTY = Float64Col()
    TOTTRDVAL = Float64Col()
    TIMESTAMP = Time64Col()
    TOTALTRADES = Float64Col()

class vixtable(IsDescription):
    SYMBOL = StringCol(32)
    TIMESTAMP = Time64Col()
    OPEN = Float64Col()
    HIGH = Float64Col()
    LOW = Float64Col()
    CLOSE = Float64Col()
    PREVCLOSE = Float64Col()
    CHANGE = Float64Col()
    PCTCHANGE = Float64Col()

def create_db():
    h5file = None
    try:
        h5file = open_file('nsedb.h5', mode='w', title='NSE bhav db')
        ftbl = h5file.create_table(h5file.root, 'fnotable', fnotable, 'NSE fno bhav table')
        stbl = h5file.create_table(h5file.root, 'stocktable', stocktable, 'NSE stock bhav table')
        itbl = h5file.create_table(h5file.root, 'indextable', indextable, 'NSE index table')
        vtbl = h5file.create_table(h5file.root, 'vixtable', vixtable, 'NSE vix table')
        print(h5file)
    except Exception as e:
        print("Error creating tables")
        print(str(e))
    finally:
        print('Closing file')
        h5file.close()
        h5file.close()

if __name__ == '__main__':
    create_db()