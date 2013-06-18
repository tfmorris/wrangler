from transform import Transform
from row import Row
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


