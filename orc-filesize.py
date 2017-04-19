import subprocess
import shlex
from datetime import date,datetime
import pdb

#print datetime.now()
with open('list_of_tables.txt','r') as f:
    lines = f.readlines()
    for i in range(0,len(lines)):
        db = lines[i].replace('\n','').split(',')[0]
        schema = lines[i].split(',')[1]
        table = lines[i].split(',')[2].replace('\n','')
        cmd = 'hdfs dfs -du -s /apps/hive/warehouse/%s.db/%s_%s'%(db.lower(),schema.lower(),table.lower())
        cmd_his = 'hdfs dfs -du -s /apps/hive/warehouse/%s_history.db/%s_%s'%(db.lower(),schema.lower(),table.lower())

        #pdb.set_trace()
        splitted = shlex.split(cmd)
        splitted_his = shlex.split(cmd_his)
        print splitted
        print splitted_his
        with open('orc-filesize_current.csv','a') as current:
            subprocess.call(splitted,stdout=current)
        with open('orc-filesize_history.csv','a') as history:
            subprocess.call(splitted_his,stdout=history)

