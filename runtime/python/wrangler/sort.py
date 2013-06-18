from transform import Transform



def mergesort(list, comparison):
    if len(list) < 2:
        return list
    else:
        middle = len(list) / 2
        left = mergesort(list[:middle], comparison)
        right = mergesort(list[middle:], comparison)
        return merge(left, right, comparison)

def merge(left, right, comparison):
	result = []
	i ,j = 0, 0
	while i < len(left) and j < len(right):
		if(comparison(left[i], right[i]) == 1):
			result.append(right[j])
			j += 1
		else:
			result.append(left[i])
			i += 1		
			
	
	result += left[i:]
	result += right[j:]
	
	
	
	return result



class Sort(Transform):
	def __init__(self):
		super(Sort,self).__init__()
		self['direction'] = []
		self['as_type'] = []

	def apply(self, tables):






		columns = self.get_columns(tables)
		table = tables[0]

		types = self['as_type']


		directions = [d for d in self['direction']]

		for d in range(len(self['direction']), len(columns)):
			directions.append('asc')
				
		directions = [(1 if d=='asc' else -1) for d in directions]
		


		def sort_fn(a, b):
			for i in range(0, len(columns)):
				col = columns[i]

				result = types[i].compare(col[a], col[b]);
				if not result == 0:
					return directions[i]*result

			if(a<b):
				return -1
			if(a==b):
				return 0
			return 1;
			
		
		# sorted_rows = mergesort(range(0, table.rows()), sort_fn)	
		sorted_rows = range(0, table.rows())
		sorted_rows.sort(sort_fn)
		
		
		
		results = [columns[0][i] for i in sorted_rows]

		new_table = table.slice(0, table.rows())

		for col in range(0, table.cols()):
			column = table[col];
			new_column = new_table[col]
			for row in range(0, table.rows()):
				new_column[row] = column[sorted_rows[row]]


		
	
	
		table.clear()
		for col in new_table:
			table.insert_column(col, {})		

		
		
	
	


