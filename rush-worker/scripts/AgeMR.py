from mrjob.job import MRJob

cols = """name,age,gender,raceethnicity,month,day,year,streetaddress,city,state,latitude,longitude,state_fp,county_fp,tract_ce,geo_id,county_id,namelsad,lawenforcementagency,cause,armed,pop,share_white,share_black,share_hispanic,p_income,h_income,county_income,comp_income,county_bucket,nat_bucket,pov,urate,college""".split(',')


class MRWordCount(MRJob):
   def mapper(self, _, line):
       row = dict(zip(cols, line.split(',')))
       if(row['age'] == "Unknown"):
           yield 0, 1
       else:
           yield int(row['age']), 1
   def reducer(self, age, counts):
      yield (age, sum(counts))

if __name__ == '__main__':
   MRWordCount.run()

command = "python3 AgeMR.py -r hadoop hdfs:///rush/input/full-data.txt | python3 AgeCalc.py"