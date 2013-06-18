from map import Map
import re
class Split(Map):
	def __init__(self):
		super(Split,self).__init__()
		self['update'] = False
		self['drop'] = True
		self['result'] = 'column'
		self.name = 'split'

	def transform(self, values):
		
		val = str(values[0])
		
		
		if(not val):
			return []

		max_splits = self['max'];
		


		#Shortcut for big splits
		if(max_splits==0 and self['on']!=None and self['before']==None and self['after']==None and self['ignore_between'] == None):
				return re.split(self['on'], val)
	
		
		
		
		
		splits = match(val, {'on':self['on'],'before':self['before'],'after':self['after'],'ignore_between':self['ignore_between'], 'max':self['max']})
		
		splitValues = []
		

		for i in range(0,len(splits)):
			if(i%2==0):
				splitValues.append(splits[i])
		


		
		return splitValues;
		
class Extract(Map):
	def __init__(self):
		super(Extract,self).__init__()
		self['update'] = False
		self['drop'] = False
		self['result'] = 'column'
		self.name = 'extract'

	def transform(self, values):

		val = str(values[0])
		splits = match(val, {'on':self['on'],'before':self['before'],'after':self['after'],'ignore_between':self['ignore_between'], 'max':self['max']})

		splitValues = []
		
		for i in range(0,len(splits)):
			if(i%2==1):
				splitValues.append(splits[i])


		return splitValues;		

class Cut(Map):
	def __init__(self):
		super(Cut,self).__init__()
		self['update'] = True
		self['drop'] = False
		self['result'] = 'column'
		self.name = 'cut'

	def transform(self, values):

		splitValues = []
		for i in range(0, len(values)):
			v = values[i]
			val = str(v)
			splits = match(val, {'on':self['on'],'before':self['before'],'after':self['after'],'ignore_between':self['ignore_between'], 'max':self['max']})

		
			x = ''
			for i in range(0,len(splits)):
				if(i%2==0):
					x += (splits[i])
			
			splitValues.append(x)

		return splitValues;		


		
		
		
		

def match(value, options):


		

	if(not value):
		return []
	
	max_splits = options['max'];

	if(max_splits==None):
		max_splits = 1

	#Shortcut for big splits
	if(options['on']!=None and options['before']==None and options['after']==None and options['ignore_between'] == None):
		if(max_splits==0):
			return re.split("(" + options['on'] + ")", value)


	remainder_to_split = value
	splits = []
	numSplit = 0;
	while(max_splits <= 0 or numSplit < max_splits*1):
		s = match_once(remainder_to_split, options)

		if(len(s) > 1):
			remainder_to_split = s[2];
			splits.append(s[0])
			splits.append(s[1])
			occurrence = 0
		else:
			break
			
		numSplit+=1
	
	splits.append(remainder_to_split)
	occurrence = 0
	newSplits = []
	prefix = ''
	which = 1
	for i in range(0, len(splits)):
		if(i%2==1):
			occurrence+=1
			if(occurrence==which):
				newSplits.append(prefix)
				newSplits.append(splits[i])
				occurrence = 0
				prefix = ''
				continue

		prefix += splits[i]

	newSplits.append(prefix)
	
	return newSplits;
	
def match_once(value, options):
	
	splits = []
	
	on = options['on']
	before = options['before']
	after = options['after']
	ignore_between = options['ignore_between']
	
	remainder = value
	remainder_offset = 0
	start_split_offset = 0
	add_to_remainder_offset = 0;
	
	while(len(remainder)):

		valid_split_region = remainder;
		valid_split_region_offset = 0;		
		start_split_offset = remainder_offset;
		if(ignore_between):
			match = re.search(ignore_between, remainder);
			if(match):
				valid_split_region = valid_split_region[0:match.start(0)]
				remainder_offset += match.index + len(match.group(0));
				remainder = remainder.substr(match.start(0)+len(match.group(0)))
			else:
				remainder = ''
		else:
			remainder = ''
	
		if(after):
			match = re.search(after, valid_split_region)
			if(match):
				valid_split_region_offset = match.start(0)+len(match.group(0));
				valid_split_region = valid_split_region[valid_split_region_offset:]
			else:
				continue;
		if(before):
			match = re.search(before, valid_split_region)
			if(match):
				valid_split_region = valid_split_region[0:match.start(0)]
			else:
				continue;
		
		
		match = re.search(on, valid_split_region)
		
		if(match):
			split_start = start_split_offset + valid_split_region_offset+match.start(0);
			split_end = split_start + len(match.group(0));

			splits.append(value[0:split_start]);
			splits.append(value[split_start:split_end])			
			splits.append(value[split_end:])
			return splits;

		else:	
			continue;
	


	return [{'start':0, 'end':len(value), 'value':value}]	