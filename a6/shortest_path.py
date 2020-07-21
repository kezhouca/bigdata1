from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_node_edges(x):
	node=int(x[0])
	edges=map(int,x[1].split())
	return (node,edges)

def distance_is_i(path,i):
	node,(source,distance)=path
	return distance==i

def transfer_to_path(x):
	node, (dest, (source, dist))= x
	return (dest,(node,dist+1))

def min_distance(x,y):
	source1,distance1=x
	source2,distance2=y
	if distance1 <= distance2:
		return x
	else: 
		return y

def output_format(path):
	node,(source,distance)=path
	return f'node {node}: source {source}, distance {distance}'
		
def main(inputs, output,source, destination):
	text = sc.textFile(inputs)
	edges=text.map(lambda line: line.split(':')).map(get_node_edges).flatMapValues(lambda x:x).cache()
	found=False
	knownPath = sc.parallelize([(source, ('-',0))])
	for i in range(6):
		edgesJoinKnownPath = edges.join(knownPath.filter(lambda path:distance_is_i(path,i)))
		newPath = edgesJoinKnownPath.map(transfer_to_path)
		if newPath.isEmpty():
			break
		knownPath = knownPath.union(newPath).reduceByKey(min_distance)
		knownPath.map(output_format).saveAsTextFile(output + '/iter-' + str(i))
		if len(newPath.lookup(destination))>0: 
			found=True
			break
	if found:
		print('Found!')
		finalPath = []
		key=destination
		while key != '-':
			value = knownPath.lookup(key)[0]
			finalPath.append((key,value))
			key,distance = value
		finalPath=finalPath[::-1]
		sc.parallelize(finalPath).map(output_format).saveAsTextFile(output + '/path')
	else:
		print('Not found!')

if __name__ == '__main__':
	conf = SparkConf().setAppName('shortest path')
	sc = SparkContext(conf=conf)
	sc.setLogLevel('WARN')
	assert sc.version >= '2.4'  # make sure we have Spark 2.4+
	inputs = sys.argv[1]+'/links-simple-sorted.txt'
	output = sys.argv[2]
	source = int(sys.argv[3])
	destination = int(sys.argv[4])
	main(inputs, output, source, destination)

