#pragma once
#pragma once
# include <iostream>
# include <string>
using namespace std;
class blockNode;
class fileNode
{
public:
	string fileName;
	fileNode *next;
	fileNode *pre;
	blockNode *blockList;
	fileNode(string fileName);
	~fileNode();
};

