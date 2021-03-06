from wrangler import dw

w = dw.DataWrangler()

# Split data repeatedly on newline  into  rows
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="row",
               update=False,
               insert_position="right",
               row=None,
               on="\n",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=0,
               positions=None,
               quote_character=None))

# Split data repeatedly on ';'
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=";",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=0,
               positions=None,
               quote_character=None))

# Split split between positions 4, 4
w.add(dw.Split(column=["split"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=None,
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=1,
               positions=[4,4],
               quote_character=None))

# Split split1 between positions 18, 18
w.add(dw.Split(column=["split1"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=None,
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=1,
               positions=[18,18],
               quote_character=None))

# Split split2 between positions 26, 26
w.add(dw.Split(column=["split2"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=None,
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=1,
               positions=[26,26],
               quote_character=None))

# Split split3 between positions 20, 20
w.add(dw.Split(column=["split3"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=None,
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=1,
               positions=[20,20],
               quote_character=None))

# Split split4 between ': ' and ' any word '
w.add(dw.Split(column=["split4"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=".*",
               before="[a-zA-Z]+",
               after=": ",
               ignore_between=None,
               which=1,
               max=1,
               positions=None,
               quote_character=None))

# Split split5 between positions 22, 22
w.add(dw.Split(column=["split5"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=None,
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=1,
               positions=[22,22],
               quote_character=None))

# Split split6 between positions 24, 24
w.add(dw.Split(column=["split6"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=None,
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=1,
               positions=[24,24],
               quote_character=None))

w.apply_to_file('andrew_kohlhoff_error_input.txt').print_csv('out.csv')
