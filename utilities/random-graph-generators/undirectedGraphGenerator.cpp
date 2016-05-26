#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <time.h>
#include <limits.h>
#include <vector>
#include <list>

using namespace std;

// #define DEBUG 1

#define K 1000
#define M 1000000

int main(int argc, char const *argv[])
{
	unsigned int nodeCount = 500 * K;
	unsigned int maxExpectedDegree = 25 * K;
	unsigned int maxEdgeWeight = 100;

	vector< list< pair<unsigned int, unsigned int> > > nodes;
	nodes.resize(nodeCount);

	unsigned int currDegreeFactor;
	unsigned int weight;
	bool first;

	srand(time(0));

	for (unsigned int i = 0; i < nodeCount; ++i)
	{
		currDegreeFactor = nodeCount / (rand() % maxExpectedDegree + 1);
		for (unsigned int j = i + 1; j < nodeCount; ++j)
		{
			if (rand() % currDegreeFactor == 0)
			{
				weight = rand() % maxEdgeWeight + 20;
				nodes[i].emplace_back(j, weight);
				nodes[j].emplace_back(i, weight);
			}
		}
	}

#ifdef DEBUG
	size_t maxDegree = 0;
	size_t minDegree = UINT_MAX;
	size_t totalEdges = 0;
#endif
	for (unsigned int i = 0; i < nodeCount; ++i)
	{
		cout << "[" << i << ",0,[";
		first = true;
		for (auto it = nodes[i].begin(); it != nodes[i].end(); ++it)
		{
			if (first) {
				cout << "[" << it->first << "," << it->second << "]";
				first = false;
			} else {
				cout << ",[" << it->first << "," << it->second << "]";
			}
		}
		cout << "]]" << endl;
#ifdef DEBUG
		maxDegree = max(maxDegree, nodes[i].size());
		minDegree = min(minDegree, nodes[i].size());
		totalEdges += nodes[i].size();
#endif
	}
#ifdef DEBUG
	cerr << "maxDegree = " << maxDegree << ", minDegree = " << minDegree << endl;
	cerr << "totalEdges = " << (totalEdges/2) << endl;
#endif
}
