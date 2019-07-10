import requests 
from requests.auth import HTTPDigestAuth
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import time

db = pyodbc.connect("Driver={SQL Server};Server=PGI-M2M;Database=M2MDATA25;Trusted_Connection=yes;")
last_upload = 0
n = 0
while True:

	query = 'select fpartno, fdescript, frev, fsource, fprodcl, FCUSRCHR3 from inmastx'
	i = pd.read_sql_query(query , db)


	i.applymap(lambda x: x.rstrip() if type(x)==str else x)
	i.replace('', np.nan, inplace=True)

	i.applymap(lambda x: x.rstrip() if type(x)==str else x)

	i['fdescript'] = i['fdescript'].str.replace(' ', '')
	i['fdescript'] = i['fdescript'].str.replace(",", " ")
	i['fpartno'] = i['fpartno'].str.replace(' ', '')
	i['fsource'] = i['fsource'].str.replace(' ', '')
	i['frev'] = i['frev'].str.replace(' ', '')
	i['fprodcl'] = i['fprodcl'].str.replace(' ', '')
	i['FCUSRCHR3'] = i['FCUSRCHR3'].str.replace(' ', '')



	i = i[i.fdescript != '**DONOTUSE**']
	i= i.drop_duplicates('fpartno',keep = 'first')
	i = i[(i['fsource']=='S') & (i['fprodcl'].str.contains("70")) | (i['fprodcl'].str.contains("40"))]

	i.drop(['fprodcl', 'fsource', 'frev'], axis=1, inplace=True)

	i = i[pd.notnull(i['fpartno'])]

	latest_data = int(i.iloc[-1].name)

	
	if last_upload<latest_data:
		print("New Data Found")

		n+=1
		last_upload = latest_data
		i.loc[[latest_data]].to_csv(f'C:/Users/tmansour/Desktop/Crons/{n}.txt', sep='\t', index = False)
		print("Data Extracted")
	time.sleep(120)