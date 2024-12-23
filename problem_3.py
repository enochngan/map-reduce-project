from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class problem_4(MRJob):
    def steps(self):
            return [
                  	MRStep(mapper = self.mapper_1,
                           	reducer = self.reducer_1),
                                                   		
                    MRStep(mapper = self.mapper_2,
                           	       reducer = self.reducer_2)
            ]

    def mapper_1(self, _, line):
        # define your first map method here
        field = line.split(",")
        email = field[4]
        if "@" in email: #extracts domain from emails
            domain = email.split("@")[-1]
            yield(domain, 1)

    def reducer_1(self, key, counts):
        # define your first reduce method here.
        yield(key, sum(counts))
        
    
    def mapper_2(self, key, line):
        # define your second map method here
        yield("total", (line, key))


    def reducer_2(self, key, counts):
        # define your second reduce method here.
        max_users = 0
        max_domain = None

        for users, domain in counts: #loops to find the domain with the max count
             if users > max_users:
                  max_users = users
                  max_domain = domain
        yield(max_domain, max_users)

if __name__ == '__main__':
    problem_4.run()