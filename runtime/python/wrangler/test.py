from wrangler import DataWrangler
import drop
from split import Split, Cut, Extract
import merge
from fill import Fill
from fold import Fold
from unfold import Unfold
from table import Table
from filter import Filter
from row import Row, RowIndex, Equals, Empty

def test():
	t = Table()


	c = t.add_column('data', {})
	
	c[0] = 'Series id:adlf\naldkjf:alkdsfsajd\nadkljfasdjf:aldkfjasdf\nlajdfjasdjf:alkdjfjas'
	
	# t[0][0] = 'a'
	# t[0][1] = 'b'
	# t[0][2] = 't'
	# t[0][3] = 'a'
	# t[0][4] = 'b'
	# t[0][5] = 't'
	# t[1][0] = 'red'
	# t[1][1] = 'red'
	# t[1][2] = 'red'
	# t[1][3] = 'blue'
	# t[1][4] = 'blue'
	# t[1][5] = 'blue'
	# t[2][0] = '1'
	# t[2][1] = '2'
	# t[2][2] = '3'
	# t[2][3] = '4'
	# t[2][4] = '5'
	# t[2][5] = '6'
	# 


 	# s = Filter()
 	# r = Row()
 	# i = Equals()
 	# i['value'] = 'red'
 	# i['lhs'] = 'color'
 	# # s['row'] = r
 	# r['conditions'] = [i]
 	# s.param('row', r)
	print 'start'
	t.debug()
	
	
	DataWrangler().add(Split().param('column', ["data"]).param('table', 0).param('status', "active").param('drop', True).param('result', "row").param('update', False).param('insert_position', "right").param('row', None).param('on', "\n").param('before', None).param('after', None).param('ignore_between', None).param('which', 1).param('max', 0).param('positions', None).param('quote_character', None)).add(Split().param('column', ["data"]).param('table', 0).param('status', "active").param('drop', True).param('result', "column").param('update', False).param('insert_position', "right").param('row', None).param('on', ":").param('before', None).param('after', None).param('ignore_between', None).param('which', 1).param('max', 1).param('positions', None).param('quote_character', None)).apply([t])
	
	
	# s.apply([t])
	print 'end'
	t.debug()

test()