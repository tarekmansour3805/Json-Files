import os
import pandas as pd
import time
import datetime
import pyodbc

db = pyodbc.connect('Driver={SQL Server}; Server=PGI-M2M; Database=M2MDATA04;Trusted_Connection=yes;')

query = 'select fpartno, fdescript, frev, fsource, fprodcl, FCUSRCHR3 from inmastx'
i = pd.read_sql_query(query, db)

print(len(i))
last_upload = 0
latest_data = int(i.iloc[-1].name)

print('starting script....')

while True:
	
	if last_upload<latest_data:
		last_upload = latest_data
		for row in latest_data(int):
			if file not in files_uploaded:
				#cursor.execute("SELECT fpartno, fdescript, frev, fsource, fprodcl, FCUSRCHR3 FROM inmastx")
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
				i = i.loc[[latest_data]]
				path=r'C:\Users\tmansour\Destop\Crons'
				i.to_csv(path+'greenl.csv')
				i.to_csv(os.path.join(path,r'green1.txt', sep='\t', index = False))



				#i.to_csv('latest.txt', sep='\t', index = False)

				files_uploaded.append(file)
				print('finished updating ' + file)
		else:
			time.sleep(2)