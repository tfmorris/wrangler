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



