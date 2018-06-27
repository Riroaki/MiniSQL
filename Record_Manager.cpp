#include <iostream>
#include "Record_Manager.h"
#include <cstring>
using namespace std;

#define RECORD_SIZE 255
#define TYPE_SIZE 30


int RecordManager::tableCreate(string tableName)
{
	string tableFileName = tableFileNameGet(tableName);

	FILE *fp;
	fp = fopen(tableFileName.c_str(), "w+");
	if (fp == NULL)
	{
		return 0;
	}
	fclose(fp);
	return 1;
}


int RecordManager::tableDrop(string tableName)
{
	string tableFileName = tableFileNameGet(tableName);
	global_buffer->deleteFileNode(tableFileName.c_str());
	if (remove(tableFileName.c_str()))
	{
		return 0;
	}
	return 1;
}


int RecordManager::recordInsert(string tableName, vector<string> record)
{
	//   fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
	blockNode *btmp = global_buffer->getHeadBlock(tableName);
	//	cout << "Get HeadNode" << endl;
	while (true)
	{
		char *content, *result;
		char insertrecord[RECORD_SIZE];
		int typeNum = 0;
		int j = 0, i = 0;

		memset(insertrecord, ' ', RECORD_SIZE);

		if (btmp == NULL)
		{
			return -1;
		}

		int numm = global_buffer->getUsedSize(btmp);
		//        cout << "Block No." << btmp->offsetNum << " has used " << numm << " before insert." << endl;


		if (BLOCK_LEN - global_buffer->getUsedSize(btmp) >= RECORD_SIZE + 2)
		{

			while (i <record.size())
			{
				for (int k = 0; k < record[i].size();k++)
				{
					insertrecord[j] = record[i][k];
					j++;
				}
				typeNum++;
				i++;
				j = typeNum*TYPE_SIZE;
			}


			result = insertrecord;
			//			cout << insertrecord << endl;
			content = strncat(global_buffer->getContent(btmp), result, RECORD_SIZE);
			//			cout << content << endl;
			global_buffer->updateBlock(tableName, content, btmp->offsetNum, RECORD_SIZE);

			char *now = global_buffer->getContent(btmp);
			//			cout << "buffer" << endl;
			//			cout << now << endl;
			int used = global_buffer->getUsedSize(btmp);
			//            cout << "Block No." << btmp->offsetNum << " has used " << used << " after insert." << endl;
			//cout << now << endl;
			return btmp->offsetNum;
		}
		else
		{
			btmp = global_buffer->getNextBlock(tableName, btmp);
		}
	}

	return -1;
}


int RecordManager::recordAllShow(string tableName, vector<Condition>* conditionVector)
{
	//    fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
	blockNode *btmp = global_buffer->getHeadBlock(tableName);
	int count = 0;
	int numm = global_buffer->getUsedSize(btmp);
	while (true)
	{
		if (global_buffer->getUsedSize(btmp) == 0)
		{
			return count;
		}
		else
		{
			int recordBlockNum = recordBlockShow(tableName, conditionVector, btmp);
			count += recordBlockNum;
			btmp = global_buffer->getNextBlock(tableName, btmp);
		}
	}

	return -1;
}


int RecordManager::recordBlockShow(string tableName, vector<Condition>* conditionVector, int blockOffset)
{
	//    fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
	blockNode* block = global_buffer->getBlockByOffset(tableName, blockOffset);
	if (block == NULL)
	{
		return -1;
	}
	else
	{
		return  recordBlockShow(tableName, conditionVector, block);
	}
}


int RecordManager::recordBlockShow(string tableName, vector<Condition>* conditionVector, blockNode* block)
{

	//if block is null, return -1
	if (block == NULL)
	{
		return -1;
	}
	int numm = global_buffer->getUsedSize(block);
	int count = 0;

	char* recordBegin = global_buffer->getContent(block);
	//    vector<Attribute> attributeVector;

	int recordSize = RECORD_SIZE;

	//    api->attributeGet(tableName, &attributeVector);
	char* blockBegin = global_buffer->getContent(block);
	size_t usingSize = global_buffer->getUsedSize(block);

	while (recordBegin - blockBegin  < usingSize)
	{
		//if the recordBegin point to a record

		if (recordConditionFit(recordBegin, recordSize, conditionVector) && *recordBegin != 0)
		{
			count++;
			recordPrint(recordBegin, recordSize);
			printf("\n");
		}

		recordBegin += recordSize;
	}

	return count;
}


int RecordManager::recordAllFind(string tableName, vector<Condition>* conditionVector)
{
	//    fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
	blockNode *btmp = global_buffer->getHeadBlock(tableName);
	int count = 0;
	while (true)
	{
		if (global_buffer->getUsedSize(btmp) == 0)
		{
			return count;
		}
		else
		{
			int recordBlockNum = recordBlockFind(tableName, conditionVector, btmp);
			count += recordBlockNum;
			btmp = global_buffer->getNextBlock(tableName, btmp);
		}
	}

	return -1;
}


int RecordManager::recordBlockFind(string tableName, vector<Condition>* conditionVector, blockNode* block)
{
	//if block is null, return -1
	if (block == NULL)
	{
		return -1;
	}
	int count = 0;

	char* recordBegin = global_buffer->getContent(block);
	//    vector<Attribute> attributeVector;
	int recordSize = RECORD_SIZE;

	//    api->attributeGet(tableName, &attributeVector);

	while (recordBegin - global_buffer->getContent(block)  < global_buffer->getUsedSize(block))
	{
		//if the recordBegin point to a record

		if (recordConditionFit(recordBegin, recordSize, conditionVector) && *recordBegin != 0)
		{
			count++;
		}

		recordBegin += recordSize;

	}

	return count;
}


int RecordManager::recordAllDelete(string tableName, vector<Condition>* conditionVector)
{
	//fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
	blockNode *btmp = global_buffer->getHeadBlock(tableName);

	int count = 0;
	while (true)
	{
		if (global_buffer->getUsedSize(btmp) == 0)
		{
			return count;
		}
		else
		{
			int recordBlockNum = recordBlockDelete(tableName, conditionVector, btmp);
			count += recordBlockNum;
			btmp = global_buffer->getNextBlock(tableName, btmp);
		}
	}

	return -1;
}


int RecordManager::recordBlockDelete(string tableName, vector<Condition>* conditionVector, int blockOffset)
{
	//    fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
	blockNode* block = global_buffer->getBlockByOffset(tableName, blockOffset);
	if (block == NULL)
	{
		return -1;
	}
	else
	{
		return  recordBlockDelete(tableName, conditionVector, block);
	}
}


int RecordManager::recordBlockDelete(string tableName, vector<Condition>* conditionVector, blockNode* block)
{
	int recordNum = 0;
	//if block is null, return -1
	if (global_buffer->getUsedSize(block) == 0)
	{
		return -1;
	}
	int count = 0;

	char* recordBegin = global_buffer->getContent(block);
	//    vector<Attribute> attributeVector;
	int recordSize = RECORD_SIZE;

	//    api->attributeGet(tableName, &attributeVector);

	while (recordBegin - global_buffer->getContent(block) < global_buffer->getUsedSize(block))
	{
		//if the recordBegin point to a record

		if (recordConditionFit(recordBegin, recordSize, conditionVector) && *recordBegin != 0)
		{
			count++;

			//            api->recordIndexDelete(recordBegin, recordSize, &attributeVector, block->offsetNum);
			int i = 0;
			char *origin = recordBegin;
			char *content;
			int num = BLOCK_LEN - RECORD_SIZE;
			int used = global_buffer->getUsedSize(block);
			int rest = BLOCK_LEN - (recordNum + 1)*(RECORD_SIZE);
			memcpy(recordBegin, recordBegin + RECORD_SIZE, rest);
			memset(recordBegin + num, 0, RECORD_SIZE);
			content = global_buffer->getContent(block);

			//            cout << "Block No." << block->offsetNum << " has used " << used << " before update." << endl;

			global_buffer->updateBlock(tableName, content, block->offsetNum, -recordSize);

			used = global_buffer->getUsedSize(block);
			//            cout << "Block No." << block->offsetNum << " has used " << used << " after update." << endl;
		}
		else
		{
			recordBegin += recordSize;
			recordNum++;
		}
	}

	return count;
}


bool RecordManager::recordConditionFit(char* recordBegin, int recordSize, vector<Condition>* conditionVector)
{
	if ((*conditionVector).size() == 0)
	{
		return true;
	}
	// int type;
	// string attributeName;
	int typeSize = TYPE_SIZE;
	char content[TYPE_SIZE];
	char *cont = content;
	char *contentBegin = recordBegin;
	Condition* con = &(*conditionVector)[0];

	if ((*conditionVector).size() == 1)
	{
		contentBegin = recordBegin + ((*conditionVector)[0].attributeIndex)*TYPE_SIZE;

		memset(content, ' ', TYPE_SIZE);
		memcpy(content, contentBegin, TYPE_SIZE);
		content[TYPE_SIZE - 1] = '\0';
		cont = content;
		if (!contentConditionFit(cont, con))
		{
			//if this record is not fit the conditon
			return false;
		}
		return true;
	}
	else if ((*conditionVector).size() == 2)
	{
		contentBegin = recordBegin + ((*conditionVector)[0].attributeIndex)*TYPE_SIZE;

		memset(content, ' ', TYPE_SIZE);
		memcpy(content, contentBegin, typeSize);
		content[TYPE_SIZE - 1] = '\0';
		cont = content;

		if (!contentConditionFit(cont, con))
		{
			//if this record is not fit the conditon
			return false;
		}

		contentBegin = recordBegin + ((*conditionVector)[1].attributeIndex)*TYPE_SIZE;

		memset(content, ' ', TYPE_SIZE);
		memcpy(content, contentBegin, typeSize);
		content[TYPE_SIZE - 1] = '\0';
		cont = content;

		con = &(*conditionVector)[1];

		if (!contentConditionFit(cont, con))
		{
			//if this record is not fit the conditon
			return false;
		}

		return true;
	}
	else
	{
		return false;
	}
}


bool RecordManager::recordPrint(char* recordBegin, int recordSize)
{
	char content[RECORD_SIZE];
	memset(content, ' ', RECORD_SIZE);
	memcpy(content, recordBegin, RECORD_SIZE);
	content[RECORD_SIZE - 1] = '\0';

	//    cout << content << endl;
	cout << "\t\t";
	string space;
	for (int i = 0;i<RECORD_SIZE;i++) {
		if (content[i] != ' ') {
			space.clear();
			int j = 0;
			while (content[i] != ' ' && i < RECORD_SIZE) {
				cout << content[i++];
				j++;
			}
			for (;j<10;j++)
				space += " ";
			cout << "  " << space;
		}
	}
	cout << endl;

	return true;
}


bool RecordManager::contentConditionFit(char* content, Condition* condition)
{
	int type = condition->operationType;
	char value[TYPE_SIZE];
	memset(value, ' ', TYPE_SIZE);
	value[TYPE_SIZE - 1] = '\0';

	const char* p = condition->cmpValue.data();
	memcpy(value, p, condition->cmpValue.size());

	if (type == 0)
	{
		if (strcmp(value, content) == 0)
			return true;
		else return false;
	}
	if (type == 5)
	{
		if (strcmp(value, content) != 0)
			return true;
		else return false;
	}

	const char *contentstr = content;
	double contentdou = atof(contentstr);

	const char *valuestr = value;
	double valuedou = atof(valuestr);

	switch (type)
	{
	case 1:
		if (contentdou > valuedou)
			return true;
		return false;
	case 2:
		if (contentdou < valuedou)
			return true;
		return false;
	case 4:
		if (contentdou <= valuedou)
			return true;
		return false;
	case 3:
		if (contentdou >= valuedou)
			return true;
		return false;
	default:
		return false;
	}
}


string RecordManager::indexFileNameGet(string indexName)
{
	string tmp = "";
	return "INDEX_FILE_" + indexName;
}


string RecordManager::tableFileNameGet(string tableName)
{
	string tmp = "";
	return tmp + "TABLE_FILE_" + tableName;
}

vector<RetRecord> RecordManager::returnRecord(string tableName, vector<Condition>* conditionVector)
{
	blockNode *btmp = global_buffer->getHeadBlock(tableName);
	vector<RetRecord> returnValue;

	while (true)
	{
		if (global_buffer->getUsedSize(btmp) == 0)
		{
			return returnValue;
		}
		else
		{
			vector<RetRecord> RecordinABlock = recordBlockReturn(tableName, conditionVector, btmp);

			if (!RecordinABlock.empty())
			{
				int num = RecordinABlock.size();
				for (int i = 0; i < num; i++)
				{
					returnValue.push_back(RecordinABlock[i]);
				}
			}

			btmp = global_buffer->getNextBlock(tableName, btmp);
		}
	}

	return returnValue;
}


/**
*
* delete record of a table in a block
* @param tableName: name of table
* @param conditionVector: the conditions list
* @param blockOffset: the block's offsetNum
* @return int: the number of the record meet requirements in the block(-1 represent error)
*/
vector<RetRecord> RecordManager::recordBlockReturn(string tableName, vector<Condition>* conditionVector, blockNode* block)
{
	vector<RetRecord> RecordinABlock;
	string type;
	int recordNum = 0;
	//if block is null, return -1
	if (global_buffer->getUsedSize(block) == 0)
	{
		return RecordinABlock;
	}
	int count = 0;

	char* recordBegin = global_buffer->getContent(block);
	//    vector<Attribute> attributeVector;
	int recordSize = RECORD_SIZE;

	//    api->attributeGet(tableName, &attributeVector);

	while (recordBegin - global_buffer->getContent(block) < global_buffer->getUsedSize(block))
	{
		//if the recordBegin point to a record

		if (recordConditionFit(recordBegin, recordSize, conditionVector) && *recordBegin != 0)
		{
			RetRecord oneWholeRecord;
			char* typeBegin = recordBegin;
			oneWholeRecord.offsetNum = block->offsetNum;
			char content[RECORD_SIZE / TYPE_SIZE][TYPE_SIZE];
			for (int i = 0; i < RECORD_SIZE / TYPE_SIZE; i++)
			{
				memset(content[i], ' ', TYPE_SIZE);
				memcpy(content[i], typeBegin, TYPE_SIZE);

				int j = 0;
				while (content[i][j] != ' ')
				{
					j++;
				}
				content[i][j] = 0;
				typeBegin += TYPE_SIZE;
			}

			int i = 0;
			while (content[i][0] != 0 && i< (RECORD_SIZE / TYPE_SIZE))
			{

				type = content[i];
				oneWholeRecord.singleRecord.push_back(type);
				i++;
			}

			RecordinABlock.push_back(oneWholeRecord);
			recordBegin += recordSize;
		}
		else
		{
			recordBegin += recordSize;
			recordNum++;
		}
	}

	return RecordinABlock;
}
