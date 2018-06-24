#pragma once
#pragma once
# include <iostream>
# include <string>
# include <cstdio>
# include "BlockNode.h"
# define BLOCK_LEN			64	
# define MAX_BLOCK			400

using namespace std;

class Buffer
{
private:
	fileNode * fileList;
	blockNode blockPool[MAX_BLOCK];
	int curBlockNum;
	int curFileNum;
	fileNode *getFile(string fileName);
	blockNode *getBlock(string fileName, int position);
	void writeToDisk(blockNode *block);
	void clearBlockNode(blockNode *block);
	void clearFileNode(fileNode *file);
	void writeAllToDisk();
public:
	Buffer();
	void deleteFileNode(string fileName);//delete file node when the file is deleted
	blockNode *getHeadBlock(string fileName);//get head block of the file for record
	blockNode *getNextBlock(string fileName, blockNode *block);//get the next of the block in the file
	int getUsedSize(blockNode *block);//return block->usedSize
	void updateBlock(string fileName, char *newData, int offset, int addSize = BLOCK_LEN);//update
	void Delete_Block(string fileName, int offset);//delete the block for index
	int createNewBlock(string fileName);//create the block for index
	char *getContent(blockNode *block);//return block->content
	blockNode *getBlockByOffset(string fileName, int offset);
	~Buffer();
};
