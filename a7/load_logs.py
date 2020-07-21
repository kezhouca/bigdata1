import sys,os,gzip,re,math,uuid
from datetime import datetime
from cassandra.cluster import Cluster,ConsistencyLevel
from cassandra.query import BatchStatement

line_re = re.compile(r'^(\S+) - - \[(\S+ [+-]\d+)\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(input_dir,keyspace,table):
	cluster = Cluster(['199.60.17.32', '199.0.17.65'])
	session = cluster.connect(keyspace)
	insert_log = session.prepare('INSERT INTO '+table+' (id,host,datetime,path,bytes) VALUES (?, ?, ?, ?, ?)')
	batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
	counter=0
	for f in os.listdir(input_dir):
		with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
			for line in logfile:
				m=line_re.match(line)
				if m is not None:
					batch.add(insert_log, (uuid.uuid4(),m.group(1),datetime.strptime(m.group(2),'%d/%b/%Y:%H:%M:%S %z'),m.group(3),int(m.group(4))))
					counter=counter+1
					if counter==300:
						session.execute(batch)
						counter=0
						batch.clear()
			session.execute(batch)#submit the rest records
			batch.clear()
			counter=0
if __name__ == '__main__':
	input_dir = sys.argv[1]
	keyspace = sys.argv[2]
	table = sys.argv[3]
	main(input_dir,keyspace,table)
