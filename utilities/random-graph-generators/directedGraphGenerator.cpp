#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <time.h>
#include <limits.h>

using namespace std;

// #define DEBUG 1

#define K 1000
#define M 1000000

int main(int argc, char const *argv[])
{
	unsigned int nodeCount = 1 * K;
	unsigned int maxExpectedDegree = 100;
	unsigned int maxEdgeWeight = 100;

#ifdef DEBUG
	size_t maxDegree = 0;
	size_t minDegree = UINT_MAX;
	size_t totalEdges = 0;
	size_t currDegree;
#endif

	unsigned int currDegreeFactor;
	bool first;

	srand(time(0));

	for (int i = 0; i < nodeCount; ++i)
	{
		cout << "[" << i << ",0,[";
		currDegreeFactor = nodeCount / (rand() % maxExpectedDegree + 1);
		first = true;
#ifdef DEBUG
		currDegree = 0;
#endif

		for (int j = 0; j < nodeCount; ++j)
		{
			if (rand() % currDegreeFactor == 0 && j != i)
			{
				if (first) {
					cout << "[" << j << "," << (rand() % maxEdgeWeight) << "]";
					first = false;
				} else {
					cout << ",[" << j << "," << (rand() % maxEdgeWeight) << "]";
				}
#ifdef DEBUG
				currDegree++;
#endif
			}
		}
#ifdef DEBUG
		maxDegree = max(maxDegree, currDegree);
		minDegree = min(minDegree, currDegree);
		totalEdges += currDegree;
#endif
		cout << "]]" << endl;
	}
#ifdef DEBUG
	cerr << "maxDegree = " << maxDegree << ", minDegree = " << minDegree << endl;
	cerr << "totalEdges = " << totalEdges << endl;
#endif
}
