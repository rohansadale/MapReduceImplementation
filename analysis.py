def process(fname):
	d1		= {}
	with open(fname,'r') as f:
		for line in f:
			tokens	= line.split(" ")
			for entry in tokens:
				entry	= entry.strip()
				if entry in d1:
					d1[entry]	= d1[entry]+1
				else:
					d1[entry]	= 0
	return d1	

f1 	= 'Data/input/200000_sorted'
f2	= 'Data/output/merge_95645550'

#f1	= 'Data/input/input_1.txt'
#f2	= 'Data/output/merge_210'

d1	= process(f1)
d2	= process(f2)

for k,v in d1.iteritems():
	if v != d2[k]:
		print "Different for key " + str(k) + " with values " + (str(v)) + " and " + str(d2[k])
