#!/bin/bash
rm dw.py
for i in 'wrangler' 'table' 'transform' 'map' 'row' 'filter' 'fill' 'fold' 'drop' 'schema' 'sort' 'split' 'unfold'
do
   cat "$i.py" | grep -v 'from [ a-zA-Z_]* import' >> dw.py
   echo "" >> dw.py
done