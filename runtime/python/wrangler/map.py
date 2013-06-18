import transform

class Map(transform.Transform):
	def __init__(self):
		super(Map,self).__init__()
		
	def apply(self, tables):
		
		columns = self.get_columns(tables)
		table = tables[0]
		
		updater = transform.TableUpdater(self, table, columns)
		

		
		for row in range(0, table.rows()):

			values = list([c[row] for c in columns])
			result = self.transform(values)
			updater.update(row, result)
		
		updater.finish()