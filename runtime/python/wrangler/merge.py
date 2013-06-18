from map import Map

class Merge(Map):
	def __init__(self):
		super(Merge,self).__init__()
		self['update'] = False
		self['drop'] = False
		self['result'] = 'column'
		self['glue'] = ''
		self.name = 'merge'

	def transform(self, values):
		return self['glue'].join(values)

		

