from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
 
# START STUDENT CODE 7.3

class wikipediaexpldataanalysis(MRJob):
    
    
    def mapper_init(self):
        pass
    
    def mapper(self, _, line): 
        node, neighbors_string = line.split('\t')
        neighbors = ast.literal_eval(neighbors_string)
        yield node, len(neighbors)
                 
    def reducer_init(self):
        self.num_nodes = 0
        self.num_links = 0
                    
    def reducer(self, key, values):
        self.num_nodes+=1
        self.num_links+= sum( values)
        
    def reducer_final(self):
        yield 'avg', (self.num_nodes, self.num_links)
        
                    
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=None,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer,
                       reducer_final=self.reducer_final)                
            ]

# END STUDENT CODE 7.3
        
if __name__ == '__main__':
    wikipediaexpldataanalysis.run()