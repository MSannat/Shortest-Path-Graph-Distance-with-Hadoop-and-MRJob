# START STUDENT CODE 7.0
# (ADD CELLS AS NEEDED)

#Working
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import sys


class BFSParallel(MRJob):
    
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=None,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer,
                       reducer_final=self.reducer_final)
                ]
    
    
    def configure_options(self):
        super(BFSParallel, self).configure_options()
        self.add_passthrough_option('--startnode', default='1', type=str, help='starting node for single source shortest path')
        self.add_passthrough_option('--endnode', default=None, type=str, help='target node to be visited')
    
    def mapper_init(self):
        self.startnode = self.options.startnode
    
    
    def mapper(self, _, line):
        
        distance = sys.maxint
        path =[]
        status= None
        node = None
        edges = None
        
        node, graph_values = line.strip().split('\t')
        node =  node.strip('"')
        values = graph_values.strip('"').split("|")
        edges = ast.literal_eval(values[0])

        if len(values) == 4:
            distance = int(values[1])
            path = ast.literal_eval(values[2])
            status = values[3]
        
        if status == None:
            if self.startnode == node:
                distance = 0
                status = 'queued'
        if status == 'queued':
            yield node, (edges, distance, path,  'visited')
            path.append(node)
            for node in edges.iterkeys():
                yield node, (None,  distance+1, path,'queued')
        else:
            yield node, (edges,  distance, path,status)



    def reducer_init(self):
        self.endnode = self.options.endnode
        self.endnode_reached = False
        self.final_path = None


    def reducer(self, key, values):
        edges = {}
        distance= []
        status = []
        path = []
        curr_state = None
        
        
        for value in values:
            edges_t = value[0]
            path_t = value[2]
            distance_t = value[1]
            status_t = value[3]
            
            if status_t == 'visited':
                path = path_t
                edges = edges_t
            else:
                if edges_t != None:
                    edges = edges_t
                if path_t != None and len(path_t) > 0:
                    path = path_t
            distance.append(distance_t)
            status.append(status_t)
        min_distance = min(distance)


        if 'visited' in status:
            curr_state = 'visited'
            if self.endnode != None and key.strip('"') == self.endnode:
                self.endnode_reached = True
                self.final_path = str(key) + "\t" + str(edges) + '|' + str(min_distance) + '|' + str(path) + '|' + "F"
        elif 'queued' in status:
            curr_state = 'queued'
        else:
            curr_state = 'unvisited'

        yield key, str(edges) + '|' + str(min_distance) + '|' + str(path) + '|' + curr_state
    
    def reducer_final(self):
        if self.endnode_reached:
            yield self.final_path.split('\t')[0], self.final_path.split('\t')[1]


if __name__ == '__main__':
    BFSParallel.run()


        
# END STUDENT CODE 7.0