from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
 
# START STUDENT CODE 7.3

class wikipediapowerlaw(MRJob):
    
    def mapper(self, _, line): 
        node, neighbors_string = line.split('\t')
        neighbors = ast.literal_eval(neighbors_string)
        yield node, len(neighbors)   
                    
    def steps(self):
        return [MRStep(mapper=self.mapper)                
            ]

# END STUDENT CODE 7.3
        
if __name__ == '__main__':
    wikipediapowerlaw.run()