from mrjob.job import MRJob
import sys

class word_count(MRJob):

    # maps each input line to a set of (word, 1) pairs, 
    # with one pair for each word in the line.
    def mapper(self, _, line):
        
        # Split the line on the spaces to get an array containing
        # the individual words.
        words = line.split(' ')
            
        # Process the words one at a time, writing a key-value pair 
        # for each of them.
        for word in words:
            yield (word, 1)

    # adds up the 1s for a given word and writes 
    # a (word, count) pair
    def reducer(self, key, counts):

        # to debug you can use lines like this:
        # sys.stdout.write(f'reducer input: {key}, {list(counts)}\n'.encode())
        
        yield (key, sum(counts))

if __name__ == '__main__':
    word_count.run()