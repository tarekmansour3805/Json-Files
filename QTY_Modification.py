import os
import pandas as pd
import time
import datetime
import pyodbc

files_uploaded = []
n_files = 0

print('starting script....')

while True:
    directory = os.listdir('C:/Users/tmansour/crons')
    print(len(directory))
    if n_files<len(directory):
        n_files = len(directory)
        for file in directory:
            if file not in files_uploaded:
                print('new update found....')
                df = pd.read_csv(f'C:/Users/tmansour/crons/{file}', sep='\t',  lineterminator='\n', names=None)
                item = df.iloc[0, 0]
                qty = df.iloc[0, 2]
                #lot = df.iloc[0, 1]
                Current_Date = datetime.datetime.today()
                conn = pyodbc.connect(r'Driver={SQL Server};Server=pgi-m2m;Database=M2Mdata25;Trusted_Connection=yes;')
                cursor = conn.cursor()
                #cursor.execute("SELECT fpartno, fqty, ftype, fctime_ts, FFROMLOC, FFROMFAC, FMATL, FUSERINFO, FTOBIN FROM intran")
                cursor.execute("SELECT fpartno, fonhand, fbinno, flocation, flot, fac FROM inonhd")
                cursor.execute(f"update inonhd SET fonhand='{qty}' WHERE fpartno = '{item}' AND fbinno = 'JUKI' AND flocation = 'JUKI' AND flot = 'JUKI'")
                conn.commit()
                conn.close()
                files_uploaded.append(file)
                print('finished updating ' + file)
                
    else:
        time.sleep(2)