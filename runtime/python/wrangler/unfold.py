from transform import Transform
import re
from table import Column, Table
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
		
		