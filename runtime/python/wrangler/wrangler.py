class DataWrangler(object):
	def __init__(self):
		self.transforms = [];
		
	def apply(self, tables):

		for t in self.transforms:
			t.apply(tables);		
		
	def add(self, t):
		self.transforms.append(t)
		return self
		
