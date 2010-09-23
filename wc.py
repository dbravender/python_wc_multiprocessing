from multiprocessing import Pool, Queue, Process
from collections import defaultdict
from glob import iglob
import re

PoolSize = 20

def count_words(filename):
    words = defaultdict(lambda: 0)
    for line in file(filename):
        for word in filter(lambda x: len(x), re.split('\W', line)):
            words[word] += 1
    return dict(words) # defaultdicts with a lambda can't be pickled

def reduce_counts(file_results, complete_results):
    totals = defaultdict(lambda: 0)
    while True:
        add_to_totals = file_results.get()
        if add_to_totals == None:
            # this is the signal that we have received all the results
            complete_results.put(dict(totals)) 
            return
        for (key, value) in add_to_totals.iteritems():
            totals[key] += value

if __name__ == '__main__':
    file_results = Queue()
    complete_results = Queue()
    reduce_process = Process(target=reduce_counts, args=(file_results, complete_results))
    reduce_process.start()
    p = Pool(PoolSize)
    # split the files up amongst workers in the process pool
    worker_results = p.imap_unordered(count_words, iglob('**/**/**'))
    for i in worker_results:
        file_results.put(i)
    # signals the end of incoming data for summing
    file_results.put(None) 
    complete_results.get()
