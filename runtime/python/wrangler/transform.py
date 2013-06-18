from table import Table, Column


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

