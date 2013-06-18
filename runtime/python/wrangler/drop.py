from transform import Transform
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


		

