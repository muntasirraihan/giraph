all : d ud

d : directedGraphGenerator.cpp
	g++ -g -std=c++11 -DDEBUG directedGraphGenerator.cpp -o directedGraphGenerator

ud : undirectedGraphGenerator.cpp
	g++ -g -std=c++11 -DDEBUG undirectedGraphGenerator.cpp -o undirectedGraphGenerator

dnd : directedGraphGenerator.cpp
	g++ -g -std=c++11 directedGraphGenerator.cpp -o directedGraphGenerator

udnd : undirectedGraphGenerator.cpp
	g++ -g -std=c++11 undirectedGraphGenerator.cpp -o undirectedGraphGenerator

clean :
	rm -r directedGraphGenerator undirectedGraphGenerator
