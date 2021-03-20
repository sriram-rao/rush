import sys
cols = """name,age,gender,raceethnicity,month,day,year,streetaddress,city,state,latitude,longitude,state_fp,county_fp,tract_ce,geo_id,county_id,namelsad,lawenforcementagency,cause,armed,pop,share_white,share_black,share_hispanic,p_income,h_income,county_i""".split(",")
def max():
    for line in sys.stdin:
        row = dict(zip(cols, line.split(',')))
        print(row)
        print(int(row['age']))

max()