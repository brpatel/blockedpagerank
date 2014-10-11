Fast Convergence PageRank in Hadoop
===============
Goal: Compute PageRank for a large Web graph (685230: nodes, 7600595: edges). This calculation has to be done using MapReduce.  This calculation has to be done using direct node by node approach and Blocked Matrix Multiplication approach. 

Design:
	A random subset of the edges were selected by using  string :dnm53 . This would reject 1% of the total edges.

	 Simple computation of PageRank involved three MapReduce tasks which will be explained in next section. The PageRanks did not converge even after fifth iteration.

	Blocked computation of PageRank involved two MapReduce tasks which will be explained in next section. The results in the result section will show that the Page ranks converge in the fifth iteration.
Simple computation of PageRank:

	First Map - PRInputMapper.java
o	The mapper reads one line at a time from edges.txt file.
o	Using netid :dnm53 and the given function, 1% of the edges are discarded.
o	It outputs key as the source node and value as the destination node.

	First reducer - PRInputReducer.java
o	The reducer will get input in the form of key, value where key will be the source node and value will be a list of destination nodes from that source node.
o	Initial page ranks of all the nodes are calculated by assigning the values to (1/N) , N being the total number of nodes in the map.
o	It outputs the key as the source node and the value as its page rank and the list of destinations concatenated in string format.
o	It writes the output in a file which will be used as input for the next Map task.

	Second Map - PRCalculateMapper.java
o	This Map task will read input from the output file of previous task.
o	It will calculate PR(u)/deg(u) for each u and emit this as the value of the output.
o	The key will be the destination node.

	Second Reducer - PRCalculateReducer.java
o	It will get the key as the destination node and value as the list of page ranks(u)/deg(u)  for the destination node from different source as the value.
o	It will emit the key as the destination node and value as string of concatenated values of all the PR(u)/deg(u) for the destination node from different source.
	Third Map - PRJoinMapper.java
o	Reads from two files that are output files of the first and second map-reduce jobs.
o	Emits node as key and a string containing its page rank and list of destination node or list of page ranks(u)/deg(u) as value, depending on whether the file was from first or second map-reduce operations.

	Third Reducer - PRJoinReducer.java
o	It takes in input with key as a node and value as list of string containing its page rank and list of destination node and list of page ranks(u)/deg(u).
o	It calculates the page rank of each node.
o	It calculates the residual as well.
o	Keep track of node count and residual sum with the help of Counters.
o	It emits the key as the node and value as a string that contains the page rank and a list of destinations ids as values.
o	This output file will be given as input to the second map task.
o	Run the 2nd and 3rd Map-Reduce functions in a loop till the residual error is less than 0.001% .

Block computation of PageRank:

Jacobi vs Gauss-Seidel

	Instead of using Jacobi's formula to calculate the page rank and achieve convergence, we experimented with Gauss-Seidel method. 
	The difference between the two is that in Gauss-Seidel,  we use the latest page rank value of the source node in the same iteration instead of waiting for the completion of the iteration to use the latest page rank.
	We observed that Gauss-Seidel allows much quicker convergence than Jacobi.
	The results are as shown  in the Results section.

Random Block Partition

	Generating a good partition is a difficult task as mentioned in the problem statement. However generating a bad one is not as hard. 
	We generated random blocks based on a simple hash function:
blockID  =  (NodeId * 67)%68
