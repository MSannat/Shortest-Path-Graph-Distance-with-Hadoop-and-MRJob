
# START STUDENT CODE 7.2 DRIVER
from BFSParallelNLTKSynonyms import BFSParallelNLTKSynonyms
import sys
import ast
import subprocess

def ind_lookup(path):
    node_words = []
    with open("indices.txt","r") as f:
        for line in f.readlines():
            word,index = line.strip().split("\t")
            if int(index) in path:
                node_words.append(word)
    return node_words 


INPUT_GRAPH = sys.argv[1]
START_NODE = sys.argv[2]
END_NODE = sys.argv[3]

mr_job = BFSParallelNLTKSynonyms(args=[INPUT_GRAPH, '--startnode', START_NODE, '--endnode', END_NODE,  '-r', 'hadoop', '--no-output', '--cleanup', 'NONE', '--no-check-input-paths', '--output-dir',  '/tmp/msannat/temp-output' ])
#mr_job = BFSParallelNLTKSynonyms(args=[INPUT_GRAPH, '--startnode', START_NODE, '--endnode', END_NODE,  '-r', 'local' ])

num_interation = 0
endnode_reached = False
#shortest_path_words = []

while (1):
    with mr_job.make_runner() as runner: 
        num_interation += 1
        print "Iteration: ", num_interation
        subprocess.Popen(["hadoop", "fs", "-rm", "-r", "/tmp/msannat/temp-output/"], stdout=None)
        runner.run()
        if num_interation ==1:
            #INPUT_GRAPH = 'undirected_toy_graph_input.txt'
            f = open(INPUT_GRAPH, 'w+')
        else:
            #INPUT_GRAPH = 'undirected_toy_graph_input.txt'
            f = open(INPUT_GRAPH, 'w+')
            
        #cat = subprocess.Popen(["hadoop", "fs", "-cat", "/tmp/msannat/temp-output/part-00000"], stdout=subprocess.PIPE)
        cat = subprocess.Popen(["hadoop", "fs", "-cat", "/tmp/msannat/temp-output/part-*"], stdout=subprocess.PIPE)
        for line in cat.stdout:
        #for line in runner.stream_output():
            f.writelines(line)
            
            line = line.split('\t')
            node = line[0].strip('"')
            data = line[1].strip('"').split('|')
            edges = data[0]
            distance = data[1]
            path = data[2]
            status = data[3].replace('"', "")

            if status[0] == 'F' :
                min_distance = distance
                endnode_reached = True
                path = ast.literal_eval(path)
                shortest_path = ' -> '.join(path) + " -> " + node
                #shortest_path_words = ' -> '.join(ind_lookup(path))  + ' -> ' + ' -> '.join(ind_lookup([node]))
                shortest_path_words = ' -> '.join(ind_lookup([int(v) for v in path]))  + ' -> ' + ' -> '.join(ind_lookup([int(node)]))

                
            
        if endnode_reached:
            break
    f.close()


print "number of iterations {}".format(num_interation)
print "shortest distance from node {} to node {} is: {} with path: [{}] and words : [{}] ".format(START_NODE, END_NODE, min_distance, shortest_path, shortest_path_words)

# END STUDENT CODE 7.2 DRIVER