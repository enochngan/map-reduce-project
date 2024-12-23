from mrjob.job import MRJob
import sys

class problem_3(MRJob):

    def mapper(self, _, line):
        # define your map method here
        field = line.split(",")
        email = field[4]
        if "@" in email:  #extracts domain from emails
            domain = email.split("@")[-1]
            yield(domain, 1)

    def reducer(self, key, counts):
        # define your reduce method here.
        yield(key, sum(counts))
        
if __name__ == '__main__':
    problem_3.run()
