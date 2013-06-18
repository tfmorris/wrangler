from transform import Transform
import re
from table import Column
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




	
