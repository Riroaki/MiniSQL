# include "BufferManager.h"
using namespace std;

Buffer::Buffer() : curBlockNum(0), curFileNum(0), fileList(NULL)
{
	for (int i = 0; i < MAX_BLOCK; i++)
	{
		blockPool[i].content = new char[BLOCK_LEN];//make space for content 
		clearBlockNode(&blockPool[i]);//initialize the blocknode
	}
}

Buffer::~Buffer()
{
	writeAllToDisk();
	for (int i = 0; i < MAX_BLOCK; i++)
	{
		delete[] blockPool[i].content;
	}
}

void Buffer::clearBlockNode(blockNode *block)
{
	memset(block->content, 0, BLOCK_LEN);
	block->dirty = false;
	block->file = NULL;
	block->next = NULL;
	block->offsetNum = -1;
	block->pre = NULL;
	block->times = 0;
	block->usedSize = 0;
}

fileNode *Buffer::getFile(string fileName)
{
	fileNode *ret = NULL;
	if (fileList)
	{
		fileNode *tempFile = fileList;
		while (tempFile->next)
		{
			if (tempFile->fileName == fileName)
			{
				return tempFile;
			}
			tempFile = tempFile->next;
		}
		if (tempFile->fileName == fileName)
		{
			return tempFile;
		}
		else
		{
			fileNode *newFile = new fileNode(fileName);
			tempFile->next = newFile;
			newFile->pre = tempFile;
			curFileNum++;
			ret = newFile;
		}
	}
	else
	{
		fileNode *newFile = new fileNode(fileName);
		fileList = newFile;
		curFileNum++;
		ret = newFile;
	}
	return ret;
}

blockNode *Buffer::getBlock(string fileName, int position)
{
	fileNode *file = getFile(fileName);
	blockNode *ret = NULL;
	for (blockNode *tempBlock = file->blockList; tempBlock; tempBlock = tempBlock->next)
	{
		if (tempBlock->offsetNum == position)
		{
			tempBlock->times++;
			return tempBlock;
		}
	}
	if (curBlockNum == 0)
	{
		ret = &blockPool[0];
		curBlockNum++;
	}
	else if (curBlockNum < MAX_BLOCK)
	{
		for (int i = 0; i < MAX_BLOCK; i++)
		{
			if (blockPool[i].offsetNum == -1)
			{
				ret = &blockPool[i];
				curBlockNum++;
				break;
			}
		}
	}
	else
	{
		blockNode *replacedBlock = &blockPool[0];
		for (int i = 0; i < MAX_BLOCK; i++)
		{
			if (replacedBlock->times >= blockPool[i].times)
			{
				replacedBlock = &blockPool[i];
			}
		}
		if (replacedBlock->pre)
		{
			replacedBlock->pre->next = replacedBlock->next;
		}
		if (replacedBlock->next)
		{
			replacedBlock->next->pre = replacedBlock->pre;
		}
		if (file->blockList == replacedBlock)
		{
			file->blockList->pre = NULL;
			file->blockList = replacedBlock->next;
		}
		writeToDisk(replacedBlock);
		clearBlockNode(replacedBlock);
		ret = replacedBlock;
	}
	ret->offsetNum = position;
	FILE *fp = fopen((file->fileName).c_str(), "rb");
	if (fp)
	{
		if (fseek(fp, ret->offsetNum * BLOCK_LEN, SEEK_SET) == 0)//删除的时候需要把后面的内容copy到前面去
		{
			int getContentSize = (int)fread(ret->content, 1, BLOCK_LEN, fp);
			if (getContentSize < 0)
			{
				cout << "write contents to block failed\n" << endl;
				return NULL;
			}
			else
			{
				if (getContentSize == 0)
				{
					ret->dirty = false;
				}
				else if (getContentSize == BLOCK_LEN)
				{
					for (int i = 0; i < BLOCK_LEN - 1; i++)
					{

						if (ret->content[i] == 0)
						{
							getContentSize--;
						}
					}
					if (ret->content[0] == 0)
					{
						getContentSize--;
					}
				}
				blockNode *tempBlock = file->blockList;
				if (tempBlock)
				{
					while (tempBlock->next)
					{
						tempBlock = tempBlock->next;
					}
					tempBlock->next = ret;
				}
				else
				{
					file->blockList = ret;
				}
				ret->pre = tempBlock;
				ret->file = file;
				ret->times = 1;
				if (getContentSize)
					ret->dirty = true;
				ret->usedSize = getContentSize;
			}
		}
		else
		{
			cout << "the file pointer wrong\n" << endl;
			return NULL;
		}
	}
	else
	{
		cout << "can not open this file\n" << endl;
		return NULL;
	}
	return ret;
}

blockNode *Buffer::getHeadBlock(string fileName)
{
	return getBlock(fileName, 0);
}

blockNode *Buffer::getNextBlock(string fileName, blockNode *block)
{
	if (block)
	{
		fileNode *file = getFile(fileName);
		blockNode *tempBlock = file->blockList;
		while (tempBlock)
		{
			if (tempBlock->offsetNum == block->offsetNum + 1)
			{
				return tempBlock;
			}
			tempBlock = tempBlock->next;
		}
		return getBlock(fileName, block->offsetNum + 1);
	}
	else
	{
		return NULL;
	}
}

blockNode *Buffer::getBlockByOffset(string fileName, int offset)
{
	blockNode *ret = NULL;
	if (offset == 0)
	{
        ret = getBlock(fileName, 0);
	}
	else
	{
		ret = getBlock(fileName, 0);
		while (offset > 0)
		{
			ret = getNextBlock(fileName, ret);
			offset--;
		}
	}
	return ret;
}

void Buffer::writeToDisk(blockNode *block)
{
	FILE *fp = fopen(block->file->fileName.c_str(), "rb+");
	if (fp)
	{
		if (fseek(fp, block->offsetNum * BLOCK_LEN, SEEK_SET) == 0)
		{
			if (fwrite(block->content, BLOCK_LEN, 1, fp) != 1)
			{
				cout << "file writing wrong\n" << endl;
				return;
			}
		}
		else
		{
			cout << "file pointer wrong\n" << endl;
			return;
		}
	}
	else
	{
		cout << "can not open the file\n" << endl;
		return;
	}
}

void Buffer::writeAllToDisk()
{
	fileNode *tempFile = fileList;
	while (tempFile)
	{
		blockNode *tempBlock = fileList->blockList;
		while (tempBlock)
		{
			writeToDisk(tempBlock);
			tempBlock = tempBlock->next;
		}
		tempFile = tempFile->next;
	}
}

void Buffer::deleteFileNode(string fileName)
{
	fileNode *tempFile = fileList;
	while (tempFile->next)
	{
		if (tempFile->fileName == fileName)
		{
			if (tempFile->pre)
				tempFile->pre->next = tempFile->next;
			if (tempFile->next)
				tempFile->next->pre = tempFile->pre;
			if (tempFile == fileList)
				fileList = tempFile->next;
			clearFileNode(tempFile);
			curFileNum--;
			return;
		}
		tempFile = tempFile->next;
	}
}

void Buffer::clearFileNode(fileNode *file)
{
	blockNode *tempBlock = file->blockList;
	while (tempBlock->next)
	{
		if (tempBlock->pre)
		{
			delete tempBlock->pre;
		}
	}
	delete tempBlock;
	file->blockList = NULL;
	file->fileName = "";
	file->next = NULL;
	file->pre = NULL;
	delete file;
}

void Buffer::updateBlock(string fileName, char *newData, int offset, int addSize)
{
	fileNode *file = getFile(fileName);
	blockNode *tempBlock = file->blockList;
	while (tempBlock)
	{
		if (tempBlock->offsetNum == offset)
		{
			tempBlock->usedSize += addSize;
			char *temp = new char[BLOCK_LEN];
			memcpy(temp, newData, BLOCK_LEN);
			memset(tempBlock->content, 0, BLOCK_LEN);
			memcpy(tempBlock->content, temp, tempBlock->usedSize);
			delete[] temp;
			if (newData[0] != '\0')
			{
				tempBlock->dirty = true;
			}
			else
			{
				tempBlock->dirty = false;
			}
			break;
		}
		tempBlock = tempBlock->next;
	}
	if (tempBlock == NULL)
	{
		tempBlock = getBlockByOffset(fileName, offset);
		tempBlock->usedSize += addSize;
		char *temp = new char[BLOCK_LEN];
		memcpy(temp, newData, BLOCK_LEN);
		memset(tempBlock->content, 0, BLOCK_LEN);
		memcpy(tempBlock->content, newData, tempBlock->usedSize);
		delete[] temp;
		if (newData[0] != '\0')
		{
			tempBlock->dirty = true;
		}
		else
		{
			tempBlock->dirty = false;
		}
	}
	writeToDisk(tempBlock);
}

void Buffer::Delete_Block(string fileName, int offset)
{
	fileNode *file = getFile(fileName);
	blockNode *tempBlock = file->blockList;
	while (tempBlock)
	{
		if (tempBlock->offsetNum == offset)
		{
			if (tempBlock->pre)
			{
				tempBlock->pre = tempBlock->next->pre;
			}
			if (tempBlock->next)
			{
				tempBlock->next = tempBlock->pre->next;
			}
			if (tempBlock == file->blockList)
			{
				file->blockList->pre = NULL;
				file->blockList = tempBlock->next;
			}
		}
		clearBlockNode(tempBlock);
		writeToDisk(tempBlock);
	}
}

int Buffer::createNewBlock(string fileName)
{
	blockNode *newBlock = getBlock(fileName, 0);
	while (1)
	{
		if (!newBlock->dirty)
		{
			return newBlock->offsetNum;
		}
		newBlock = getNextBlock(fileName, newBlock);
	}
}

int Buffer::getUsedSize(blockNode *block)
{
	return block->usedSize;
}

char *Buffer::getContent(blockNode *block)
{
	return block->content;
}
