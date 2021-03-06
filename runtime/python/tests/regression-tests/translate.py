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

# Split data repeatedly on ','
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="column",
               update=False,
               insert_position="right",
               row=None,
               on=",",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=0,
               positions=None,
               quote_character=None))

# Translate split1 down
w.add(dw.Translate(column=["split1"],
                   table=0,
                   status="active",
                   drop=False,
                   direction="down",
                   values=1))

# Translate split1 up
w.add(dw.Translate(column=["split1"],
                   table=0,
                   status="active",
                   drop=False,
                   direction="up",
                   values=1))

w.apply_to_file('crime.csv').print_csv('out.csv')
