from mrjob.job import MRJob
from io import StringIO
import csv

cols = """name,age,gender,raceethnicity,month,day,year,streetaddress,city,state,latitude,longitude,state_fp,county_fp,tract_ce,geo_id,county_id,namelsad,lawenforcementagency,cause,armed,pop,share_white,share_black,share_hispanic,p_income,h_income,county_income,comp_income,county_bucket,nat_bucket,pov,urate,college""".split(',')
class MRWordCount(MRJob):


   def mapper(self, _, line):
       x = StringIO(line)
       reader = csv.reader(x, delimiter=',')
       for r in reader:
          row = dict(zip(cols, r))
 #     print(row)
       yield (row['cause'], 1)

   def reducer(self, word, counts):
      yield(word, sum(counts))

if __name__ == '__main__':
   MRWordCount.run()