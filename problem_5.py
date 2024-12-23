from mrjob.job import MRJob
import sys

class problem_6(MRJob):

    def mapper(self, _, line):
        # define your map method here
        field1 = line.split(",")
        dob = field1[3]

        field2 = line.split(";") #splits the group field and stores in group_info
        group_info = field2[0]
        groups = group_info.split(",")[5:]

        birth_year = int(dob.split("-")[0]) #gets dob

        older_60 = birth_year <= 1963 #determines if user is older than 60

        for group in groups:
            if older_60: #if older than 60
                yield(group, 1)
            else: 
                yield(group, 0)

    def reducer(self, key, counts):
        # define your reduce method here.
        yield(key, sum(counts))
        
if __name__ == '__main__':
    problem_6.run()