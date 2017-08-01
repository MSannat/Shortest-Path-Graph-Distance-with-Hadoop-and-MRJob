from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
 
# START STUDENT CODE 7.1

class nltksynonymspowerlaw(MRJob):
    
    def mapper(self, _, line): 
        node, neighbors_string = line.split('\t')
        neighbors = ast.literal_eval(neighbors_string)
        yield node, len(neighbors)   
                    
    def steps(self):
        return [MRStep(mapper=self.mapper)                
            ]

# END STUDENT CODE 7.1
        
if __name__ == '__main__':
    nltksynonymspowerlaw.run()