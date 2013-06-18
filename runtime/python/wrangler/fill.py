from transform import Transform
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



