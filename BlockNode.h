#pragma once
# include "FileNode.h"
# include <iostream>
using namespace std;
class blockNode
{
public:
	int offsetNum;
private:
	char *content;
	fileNode *file;
	int times;
	blockNode *next;
	blockNode *pre;
	bool dirty;
	int usedSize;
	friend class Buffer;
};