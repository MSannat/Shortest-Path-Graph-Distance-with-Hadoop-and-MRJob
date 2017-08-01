
from BFSParallel import BFSParallel
import sys
import ast
import subprocess

INPUT_GRAPH = sys.argv[1]
START_NODE = sys.argv[2]
END_NODE = sys.argv[3]

mr_job = BFSParallel(args=[INPUT_GRAPH, '--startnode', START_NODE, '--endnode', END_NODE,  '-r', 'hadoop', '--no-output', '--cleanup', 'NONE', '--no-check-input-paths', '--output-dir',  '/tmp/msannat/temp-output' ])
#mr_job = BFSParallel(args=[INPUT_GRAPH, '--startnode', START_NODE, '--endnode', END_NODE,  '-r', 'local' ])

num_interation = 0
endnode_reached = False

while (1):
    with mr_job.make_runner() as runner: 
        num_interation += 1
        print "Iteration: ", num_interation
        subprocess.Popen(["hadoop", "fs", "-rm", "-r", "/tmp/msannat/temp-output/"], stdout=None)
        runner.run()
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
            
        if endnode_reached:
            break
    f.close()

print "number of iterations {}".format(num_interation)
print "shortest distance from node {} to node {} is: {} with path: [{}]".format(START_NODE, END_NODE, min_distance, shortest_path)