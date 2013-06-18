import dw
import sys

if(len(sys.argv) < 3):
	sys.exit('Error: Please include an input and output file.  Example python script.py input.csv output.csv')

input_file = open(sys.argv[1], 'r')
output_file = open(sys.argv[2], 'w')
input_text = input_file.read()
t = dw.Table()
c = t.add_column('data', {})
c[0] = input_text

w = dw.DataWrangler()
w.add(dw.Split().param('column', ["data"]).param('table', 0).param('status', "active").param('drop', True).param('result', "row").param('update', False).param('insert_position', "right").param('row', None).param('on', "\n").param('before', None).param('after', None).param('ignore_between', None).param('which', 1).param('max', 0).param('positions', None).param('quote_character', None))
w.add(dw.Split().param('column', ["data"]).param('table', 0).param('status', "active").param('drop', True).param('result', "column").param('update', False).param('insert_position', "right").param('row', None).param('on', ",").param('before', None).param('after', None).param('ignore_between', None).param('which', 1).param('max', 0).param('positions', None).param('quote_character', None))

w.apply([t])
output_file.write(t.csv())

