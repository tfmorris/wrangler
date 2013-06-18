from transform import Transform
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
