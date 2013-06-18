class DataWrangler(object):
	def __init__(self):
		self.transforms = [];
		
	def apply(self, tables):

		for t in self.transforms:
			t.apply(tables);		
		
	def add(self, t):
		self.transforms.append(t)
		return self
		

import string
class Table:
	def __init__(self):
		self.data = []
		self.data_by_name = {}
		self.name_counts = {}

	def clear(self):
		self.data = []
		self.name_counts = {}
		self.data_by_name = {}
	
	def remove_column(self, col):
		del self.data_by_name[col.name]
		self.data.remove(col)

	
	def add_column(self, name, options):
		name = self.clean_name(name)
		col = Column(name, 't', 'r')
		self.data.append(col)
		self.data_by_name[col.name] = col
		
		return col

	def insert_column(self, col, options):
		c = self.add_column(col.name, options)
		for i in range(0, len(col.data)):
			c[i] = col[i]


	def slice(self, start, end):
		new_table = Table()
		for col in self:
			new_col = new_table.add_column(col.name, {})
	
		return new_table
		
	def append(self, other):
		for col in other:
			self.insert_column(col, {})
			

	def clean_name(self, name):
		if(name==None):
			name = '_'
		name = string.replace(name, ' ', '_')
		
		clean = name
		while(self[clean]!=None):

			try:			
				count = self.name_counts[name];

				self.name_counts[name] += 1
			except KeyError:
				count = 0;
				self.name_counts[name]=1

			
			clean = name + str(self.name_counts[name]);
			
			

		return clean;

	def index(self, col):
		return self.data.index(col)
		

	def set_name(self, col, name):
		old_name = col.name
		
		name = self.clean_name(name)
		
		col.name = name
		del self.data_by_name[old_name]
		
		self.data_by_name[name] = col

	def __getitem__(self, i):
		try:
			if(isinstance(i, str)):
				return self.data_by_name[i]
			else:
				return self.data[i]
		except KeyError:
			return
	def rows(self):
		return len(self.data[0].data) if len(self.data) else 0
	def cols(self):
		return len(self.data)

	def row(self,r):
		return [c[r] for c in self.data]


	def csv(self):

		x = [','.join([c.name for c in self.data])+'\n']

		for row in range(0, self.rows()):
			vals = self.row(row)
			vals = ['"'+v+'"' if v else '' for v in vals]
			x.append(','.join(vals) +'\n')
			
		return ''.join(x)

	def debug(self):
		
		print '\t'.join([c.name for c in self.data])+'\n'
		
		for row in range(0, self.rows()):
			vals = self.row(row)
			vals = [v if v else '' for v in vals]
			print '\t'.join(vals) +'\n'
		
class Column:
	def __init__(self, n, t, r):
		self.data = []
		self.name = n
		self.type = t
		self.role = r
	
	def __getitem__(self, i):
		try:
			return self.data[i]
		except IndexError:
			l = len(self.data)
			if(i >= l):
				for j in range(l, i+1):
					self.data.append('')
			return ''
	def __setitem__(self, i, v):
		l = len(self.data)
		if(i >= l):
			for j in range(l, i+1):
				self.data.append('')
		
		self.data[i] = v

	def splice(self, i):
		del self.data[i]
	def append(self, v):
		self.data.append(v)






class Transform(object):
	def __init__(self):
		self.parameters = {};
		
	def __getitem__(self, i):
		try:
			return self.parameters[i]
		except KeyError:
			return None
		
	def __setitem__(self, i, val):
		self.parameters[i] = val
		
	def get_columns(self, tables):
		table = tables[0]
		
		cols = self['column']
		if(not cols or len(cols)==0):
			cols = [c.name for c in table]
		

		return [table[c] for c in cols]
		
	def param(self, key, value):
		self[key] = value
		return self



class TableUpdater(object):
	
	def finish(self):

		if(self.transform['result']=='column' and self.transform['drop']):
			for col in self.columns:
				self.table.remove_column(col)
		
		
		if(self.transform['result']=='row'):
			self.table.clear()
			for col in self.split_cols:
				c = self.table.insert_column(col, {})



	def update(self, row, values):
		if(self.transform['result']=='row'):
			updateIndex = self.update_index
			cols = self.split_cols
			for i in range(0, len(values)):
				for c in range(0, updateIndex):
					cols[c][self.new_index] = table[c][row];

				for c in range(updateIndex+1, len(cols)):
					cols[c][self.new_index] = table[c][row];		

				cols[updateIndex][self.new_index] = values[i]
				self.new_index += 1;
		
		else:
			insertPosition = self.insert_position
			if(self.transform['update']):
				for i in range(0, len(values)):
					self.columns[i][row] = values[i];										
			else:
				for i in range(insertPosition, insertPosition+len(values)):
					if(i == self.table.cols()):
						newCol = self.table.add_column(self.transform.name, {'index':insertPosition+len(self.new_columns)});
						# self.new_columns.append(newCol)
					self.table[i][row] = values[i-insertPosition];

			
			
	def __init__(self, transform, table, columns):
		self.transform = transform
		self.table = table
		self.columns = columns
		self.new_columns = []
		self.new_index = 0
		self.insert_position = table.cols()
		if(len(columns)):
			self.update_index = table.index(columns[0])
			self.split_cols = [Column(col.name,'','') for col in table]



class Map(Transform):
	def __init__(self):
		super(Map,self).__init__()
		
	def apply(self, tables):
		
		columns = self.get_columns(tables)
		table = tables[0]
		
		updater = TableUpdater(self, table, columns)
		

		
		for row in range(0, table.rows()):

			values = list([c[row] for c in columns])
			result = self.transform(values)
			updater.update(row, result)
		
		updater.finish()

import re, math
class Row(Transform):
	def __init__(self):
		super(Row,self).__init__()
		self['conditions'] = []
		self.name = 'row'

	def test(self, tables, row):
		conditions = self['conditions']

		for cond in conditions:
			if(not cond.test(tables, row)):
				return False
		
		return True


class RowIndex(Transform):
	def __init__(self):
		super(RowIndex,self).__init__()
		self['indices'] = []
		self.name = 'row_index'

	def test(self, tables, row):
		return row in self['indices']

class Empty(Transform):
	def __init__(self):
		super(Empty,self).__init__()
		self.name = 'empty'

	def test(self, table, row):
		for c in table:
			v = c[row]
			if(not v == None and len(str(v))):
				return False
		return True

class IsNull(Transform):
	def __init__(self):
		super(IsNull,self).__init__()
		self.name = 'is_null'

	def test(self, table, row):
		x = table[self['lcol']][row]
		return x == None or len(str(x)) == 0	


class IsType(Transform):
	def __init__(self):
		super(IsType,self).__init__()
		self.name = 'is_type'

	def test(self, table, row):
		return self['type'].parse(table[self['lcol']][row]) == None

class Type(Transform):
	def __init__(self):
		super(Type,self).__init__()
		self.name = 'type'

	def transform(self, values):
		return [self.parse(v) for v in values]

	def compare(self,a,b):
		a = self.parse(a)
		b = self.parse(b)
		if(a < b):
			return -1
		if(a==b):
			return 0
		if(a > b):
			return 1


class Number(Type):
	def __init__(self):
		super(Number,self).__init__()
		self.name = 'number'

	def parse(self, v):
		try:
			return float(v)
		except ValueError:
			return None

class Integer(Type):
	def __init__(self):
		super(Integer,self).__init__()
		self.name = 'integer'


	def parse(self, v):
		try:
			x = float(v)
			return x == math.floor(x)
		except ValueError:
			return None

class String(Type):
	def __init__(self):
		super(String,self).__init__()
		self.name = 'string'


	def parse(self, v):
		return v

class Date(Type):
	def __init__(self):
		super(String,self).__init__()
		self.name = 'string'


	def parse(self, v):
		try:
			return parser.parse(v)
		except ValueError:
			return None				

class Equals(Transform):
	def __init__(self):
		super(Equals,self).__init__()
		self.name = 'equals'

	def test(self, table, row):
		return table[self['lcol']][row] == self['value']


class Contains(Transform):
	"""docstring for Contains"""
	def __init__(self):
		super(Contains, self).__init__()
		self.name = 'contains'

	def test(self, table, row):
		return re.search(self['value'], str(table[self['lcol']][row])) 


class StartsWith(Transform):
	"""docstring for StartsWith"""
	def __init__(self):
		super(StartsWith, self).__init__()
		self.name = 'starts_with'

	def test(self, table, row):
		return re.match(self['value'], str(table[self['lcol']][row]))
		

import re

class Filter(Transform):
	def __init__(self):
		super(Filter,self).__init__()
		self['row'] = Row()
		self.name = 'filter'

	def apply(self, tables):

		columns = self.get_columns(tables)
		table = tables[0]

		filtered_table = table.slice(0, 0)
		row = self['row']
		start_row = 0
		end_row = table.rows()


		for r in range(start_row, end_row):
			if(row.test(table, r)):
				pass
			else:
				for col in table:
					filtered_col = filtered_table[col.name]
					filtered_col.append(col[r])
		
		
		table.clear()
		
		table.append(filtered_table)



import re
class Fill(Transform):
	def __init__(self):
		super(Fill,self).__init__()
		self['direction'] = 'down'
		self.name = 'fill'
	def apply(self, tables):

		columns = self.get_columns(tables)
		table = tables[0]

		start_row = 0
		end_row = table.rows()
		direction = self['direction']
		
		row = self['row']
		
		if(direction=='down' or direction=='up'):
			row_range = range(start_row, end_row)
			if(direction=='up'):
				row_range.reverse()
			for col in columns:
				fillValue = None
				for r in row_range:
					v = col[r]
					if(v == None or v==''):
						col[r] = fillValue
					else:
						if(row and row.test(table, r)):
							fillValue = v

		if(direction=='left' or direction=='right'):
			col_range = range(0, len(columns))
			if(direction=='left'):
				col_range.reverse()
			for r in range(start_row, end_row):
				if(row and row.test(table, r)):
					fillValue = None
					for c in col_range:
						v = columns[c][r]
						if(v == None or v==''):
							columns[c][r] = fillValue
						else:
							fillValue = v




import re
class Fold(Transform):
	def __init__(self):
		super(Fold,self).__init__()
		self['drop'] = False
		self['keys'] = [-1]
		self.name = 'fold'

	def apply(self, tables):

		columns = self.get_columns(tables)
		table = tables[0]
		
		new_index = 0
		names = [c.name for c in columns]
		rows = table.rows()
		start_row = 0
		end_row = table.rows()
		keys = self['keys']
		# These are the keys to use for the fold...We use the header if the key = -1 otherwise we use the value in the cell

		
		
		key_values = []
		for col in columns:
			col_key_vals = []
			for key in keys:
				val = (col.name if key==-1 else col[key])
				col_key_vals.append(val)
				key_values.append(col_key_vals)
					


		
		foundLeft = False;
		other_cols = []
		

		
		for col in table:

			if(col.name in names):
				if(not foundLeft):
					update_col = col
				foundLeft = True				
			else:
				other_cols.append(col)

		key_cols = [table.add_column('fold', {}) for k in keys]
		value_col = table.add_column('value', {})
		other_cols = [Column(c.name, 't','r') for c in other_cols]

		

		for row in range(start_row, end_row):
			if(not row in keys):
				for k in range(0, len(columns)):
					for c in range(0, len(other_cols)):
						col = other_cols[c]
						
						col[new_index] = table[col.name][row]
					
					for j in range(0, len(key_cols)):
						key_cols[j][new_index] = key_values[k][j]
					
					value_col[new_index] = columns[k][row]
					new_index+=1;
				
			
		



		updateIndex = table.index(update_col) if update_col else 0;



		table.clear()


		key_cols.append(value_col)
		

		for col in other_cols:
			table.insert_column(col, {'index':updateIndex})		
		
		for col in key_cols:
			table.insert_column(col, {})




	

import re
class Drop(Transform):
	def __init__(self):
		super(Drop,self).__init__()
		self['drop'] = True
		self.name = 'drop'
	def apply(self, tables):

		columns = self.get_columns(tables)
		table = tables[0]


		
		if(self['drop']):
			for col in columns:
				table.remove_column(col);


		


import re
class SetName(Transform):
	def __init__(self):
		super(SetName,self).__init__()
		self.name = 'SetName'
		self['drop'] = True
	def apply(self, tables):

		columns = self.get_columns(tables)
		table = tables[0]


		if(self['header_row']):
			row = table.row(self['header_row'])
			i = 0
			for c in table:
				val = row[i];
				if(val==None or len(val)==0):
					val = 'undefined';
				table.set_name(c, '_'+val)

				if(t['drop']):
					c.splice(t._header_row)					
				
				i+=1
			else:
				for col in columns:
					table.set_name(col + '_'+names[i])




def mergesort(list, comparison):
    if len(list) < 2:
        return list
    else:
        middle = len(list) / 2
        left = mergesort(list[:middle], comparison)
        right = mergesort(list[middle:], comparison)
        return merge(left, right, comparison)

def merge(left, right, comparison):
	result = []
	i ,j = 0, 0
	while i < len(left) and j < len(right):
		if(comparison(left[i], right[i]) == 1):
			result.append(right[j])
			j += 1
		else:
			result.append(left[i])
			i += 1		
			
	
	result += left[i:]
	result += right[j:]
	
	
	
	return result



class Sort(Transform):
	def __init__(self):
		super(Sort,self).__init__()
		self['direction'] = []
		self['as_type'] = []

	def apply(self, tables):






		columns = self.get_columns(tables)
		table = tables[0]

		types = self['as_type']


		directions = [d for d in self['direction']]

		for d in range(len(self['direction']), len(columns)):
			directions.append('asc')
				
		directions = [(1 if d=='asc' else -1) for d in directions]
		


		def sort_fn(a, b):
			for i in range(0, len(columns)):
				col = columns[i]

				result = types[i].compare(col[a], col[b]);
				if not result == 0:
					return directions[i]*result

			if(a<b):
				return -1
			if(a==b):
				return 0
			return 1;
			
		
		# sorted_rows = mergesort(range(0, table.rows()), sort_fn)	
		sorted_rows = range(0, table.rows())
		sorted_rows.sort(sort_fn)
		
		
		
		results = [columns[0][i] for i in sorted_rows]

		new_table = table.slice(0, table.rows())

		for col in range(0, table.cols()):
			column = table[col];
			new_column = new_table[col]
			for row in range(0, table.rows()):
				new_column[row] = column[sorted_rows[row]]


		
	
	
		table.clear()
		for col in new_table:
			table.insert_column(col, {})		

		
		
	
	



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

import re
class Unfold(Transform):
	def __init__(self):
		super(Unfold,self).__init__()
		self['drop'] = False
		self['measure'] = []
		self['column'] = []
		self.name = 'Unfold'

	def apply(self, tables):

		columns = self.get_columns(tables)
		table = tables[0]
		
		measure_column = table[self['measure']]
		
		new_column_headers = []
		header_column = columns[0]
		start_row = 0
		end_row = table.rows()

		for v in header_column.data:
			if(not v in new_column_headers):
				new_column_headers.append(v)
		

		
		key_columns = filter(lambda x: not x.name in [measure_column.name, header_column.name], table)

		reduction = {}
		reduction_index = 0
		
		new_table = Table()
		
		for col in key_columns:
			new_table.add_column(col.name, {})
		
		#this name lookup is needed since table implementation changes names under certain circumstances
		name_lookup = {}
		for header in new_column_headers:
			col = new_table.add_column(header, {})
			name_lookup[header] = col.name
		
		
		for row in range(start_row, end_row):
			key = '*'.join([col[row] for col in key_columns])
			if(not reduction.has_key(key)):
				reduction[key] = reduction_index
				
				for col in key_columns:
					new_table[col.name][reduction_index] = col[row]
				
				reduction_index += 1

			index = reduction[key]
			header = header_column[row]
			measure = measure_column[row]
			
			
			new_table[name_lookup[header]][index] = measure


		table.clear()
		
		for col in new_table:
			table.insert_column(col, {})
		
		

