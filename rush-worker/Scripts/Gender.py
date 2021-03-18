from mrjob.job import MRJob
import csv

cols = """name,age,gender,raceethnicity,month,day,year,streetaddress,city,state,latitude,longitude,state_fp,county_fp,tract_ce,geo_id,county_id,namelsad,lawenforcementagency,cause,armed,pop,share_white,share_black,share_hispanic,p_income,h_income,county_income,comp_income,county_bucket,nat_bucket,pov,urate,college""".split(',')
class MRWordCount(MRJob):


   def mapper(self, _, line):
       row = dict(zip(cols, line.split(',')))
 #     print(row)
       yield (row['gender'], 1)

   def reducer(self, word, counts):
      yield(word, sum(counts))

if __name__ == '__main__':
   MRWordCount.run()