# include "FileNode.h"

using namespace std;

fileNode::fileNode(string fileName) :fileName(fileName), pre(NULL), next(NULL), blockList(NULL)
{}

fileNode::~fileNode() {}
