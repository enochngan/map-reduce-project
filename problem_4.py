from mrjob.job import MRJob
import sys

class problem_5(MRJob):

    def mapper(self, _, line):
        # define your map method here
        field1 = line.split(",")
        id = field1[0]

        field2 = line.split(";") #splits the friends field and stores in friends
        friends = field2[-1]
        
        if friends: #counts friends if there is a field
            num_friends = len(friends.split(","))
        else:
            num_friends = 0

        yield("total", (num_friends, id))

    def reducer(self, key, counts):
        # define your reduce method here.
        max_friends = 0
        max_id = None

        for num_friends, id in counts: #loops to find domain with most friends
            if num_friends > max_friends:
                max_friends = num_friends
                max_id = id
        yield(max_id, max_friends)
        
if __name__ == '__main__':
    problem_5.run()