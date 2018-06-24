#include <iostream>
#include "Record_Manager.h"
#include <cstring>
//#include "API.h"
; using namespace std;

#define RECORD_SIZE 255
#define TYPE_SIZE 30
/**
 *
 * create a table
 * @param tableName name of table
 */
int RecordManager::tableCreate(string tableName)
{
    string tableFileName = tableFileNameGet(tableName);
//	cout << tableFileName << endl;
    
    FILE *fp;
    fp = fopen(tableFileName.c_str(), "w+");
    if (fp == NULL)
    {
        return 0;
    }
    fclose(fp);
    return 1;
}

/**
 *
 * drop a table
 * @param tableName name of table
 */
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

/**
 *
 * create a index
 * @param indexName name of index
 */
// int RecordManager::indexCreate(string indexName)
// {
//     string indexFileName = indexFileNameGet(indexName);
    
//     FILE *fp;
//     fp = fopen(indexFileName.c_str(), "w+");
//     if (fp == NULL)
//     {
//         return 0;
//     }
//     fclose(fp);
//     return 1;
// }

/**
 *
 * drop a index
 * @param indexName name of index
 */
// int RecordManager::indexDrop(string indexName)
// {
//     string indexFileName = indexFileNameGet(indexName);
//     global_buffer->deleteFileNode(indexFileName.c_str());
//     if (remove(indexFileName.c_str()))
//     {
//         return 0;
//     }
//     return 1;
// }

/**
 *
 * insert a record to table
 * @param tableName name of table
 * @param record value of record
 * recordSize size of the record
 * @return the position of block in the file(-1 represent error)
 */
int RecordManager::recordInsert(string tableName, vector<string>* record)
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
		cout << "Block No." << btmp->offsetNum << " has used " << numm << " before insert." << endl;

        if (BLOCK_LEN-global_buffer->getUsedSize(btmp) >=RECORD_SIZE+2)
        {   
            while(i<=(*record)[0].size())
            {
                while((*record)[0][i]!=','&&i<(*record)[0].size())
                {
                    insertrecord[j]=(*record)[0][i];
//					cout << insertrecord[j] << endl;
                    i++;
                    j++;
                }
                typeNum++;
				i++;
                j=typeNum*TYPE_SIZE;
            }
//			insertrecord[RECORD_SIZE - 1] = '\0';
			if (numm == 0)
			{
//				insertrecord[RECORD_SIZE-1] = '\0';
			}
//			memset(insertrecord + RECORD_SIZE, '\0', 1);
			result = insertrecord;
//			cout << insertrecord << endl;
			content = strncat(global_buffer->getContent(btmp), result, RECORD_SIZE);
//			cout << content << endl;
            global_buffer->updateBlock(tableName, content, btmp->offsetNum, RECORD_SIZE);

//            char *now = global_buffer->getContent(btmp);
            global_buffer->getContent(btmp);
//			cout << "buffer" << endl;
//			cout << now << endl;
			int used = global_buffer->getUsedSize(btmp);
			cout << "Block No." << btmp->offsetNum << " has used " << used<<" after insert." << endl;
            return btmp->offsetNum;
        }
        else
        {
            btmp = global_buffer->getNextBlock(tableName, btmp);
        }
    }
    
    return -1;
}

/**
 *
 * print all record of a table meet requirement
 * @param tableName name of table
 *  attributeNameVector the attribute list
 * @param conditionVector the conditions list
 * @return int the number of the record meet requirements(-1 represent error)
 */
int RecordManager::recordAllShow(string tableName, vector<Condition>* conditionVector)
{
//    fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
    blockNode *btmp = global_buffer->getHeadBlock(tableName);
    int count = 0;
//    int numm = global_buffer->getUsedSize(btmp);
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

/**
 *
 * print record of a table in a block
 * @param tableName name of table
 * attributeNameVector the attribute list
 * @param conditionVector the conditions list
 * @param blockOffset the block's offsetNum
 * @return int the number of the record meet requirements in the block(-1 represent error)
 */
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

/**
 *
 * print record of a table in a block
 * @param tableName name of table
 * attributeNameVector the attribute list
 * @param conditionVector the conditions list
 * @param block search record in the block
 * @return int the number of the record meet requirements in the block(-1 represent error)
 */
int RecordManager::recordBlockShow(string tableName, vector<Condition>* conditionVector, blockNode* block)
	{
    
    //if block is null, return -1
    if (block == NULL)
    {
        return -1;
    }
//    int numm=global_buffer->getUsedSize(block);
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
        
        if(recordConditionFit(recordBegin, recordSize, conditionVector))
        {
            count ++;
            recordPrint(recordBegin, recordSize);
            printf("\n");
        }
        
        recordBegin += recordSize;
    }
    
    return count;
}

/**
 *
 * find the number of all record of a table meet requirement
 * @param tableName name of table
 * @param conditionVector the conditions list
 * @return int the number of the record meet requirements(-1 represent error)
 */
int RecordManager::recordAllFind(string tableName, vector<Condition>* conditionVector)
{
//    fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
    blockNode *btmp = global_buffer->getHeadBlock(tableName);
    int count = 0;
    while (true)
    {
        if (btmp==NULL)
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

/**
 *
 * find the number of record of a table in a block
 * @param tableName name of table
 * @param block search record in the block
 * @param conditionVector the conditions list
 * @return int the number of the record meet requirements in the block(-1 represent error)
 */
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
        
        if(recordConditionFit(recordBegin, recordSize, conditionVector))
        {
            count++;
        }
        
        recordBegin += recordSize;
        
    }
    
    return count;
}

/**
 *
 * delete all record of a table meet requirement
 * @param tableName name of table
 * @param conditionVector the conditions list
 * @return int the number of the record meet requirements(-1 represent error)
 */
int RecordManager::recordAllDelete(string tableName, vector<Condition>* conditionVector)
{
    //fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
    blockNode *btmp = global_buffer->getHeadBlock(tableName);

    int count = 0;
    while (true)
    {
        if (global_buffer->getUsedSize(btmp)==0)
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

/**
 *
 * delete record of a table in a block
 * @param tableName name of table
 * @param conditionVector the conditions list
 * @param blockOffset the block's offsetNum
 * @return int the number of the record meet requirements in the block(-1 represent error)
 */
int RecordManager::recordBlockDelete(string tableName,  vector<Condition>* conditionVector, int blockOffset)
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

/**
 *
 * delete record of a table in a block
 * @param tableName name of table
 * @param conditionVector the conditions list
 * @param block search record in the block
 * @return int the number of the record meet requirements in the block(-1 represent error)
 */
int RecordManager::recordBlockDelete(string tableName,  vector<Condition>* conditionVector, blockNode* block)
{
	int recordNum = 0;
    //if block is null, return -1
    if (global_buffer->getUsedSize(block)==0)
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
        
        if(recordConditionFit(recordBegin, recordSize, conditionVector))
        {
            count ++;
            
//            api->recordIndexDelete(recordBegin, recordSize, &attributeVector, block->offsetNum);
//            int i = 0;
//            char *origin = recordBegin;
            char *content;
			int num = BLOCK_LEN - RECORD_SIZE;
			int used = global_buffer->getUsedSize(block);
			int rest = BLOCK_LEN - (recordNum+1)*(RECORD_SIZE);
			memcpy(recordBegin, recordBegin + RECORD_SIZE,rest);
            memset(recordBegin+num,0, RECORD_SIZE);
            content=global_buffer->getContent(block);

			cout << "Block No." << block->offsetNum << " has used " << used <<" before update."<< endl;

            global_buffer->updateBlock(tableName, content, block->offsetNum, -recordSize);
			
			used = global_buffer->getUsedSize(block);
			cout << "Block No." << block->offsetNum << " has used " << used<<" after update."<< endl;
        }
        else
        {
            recordBegin += recordSize;
			recordNum++;
        }
    }
    
    return count;
}

/**
 *
 * insert the index of all record of the table
 * @param tableName name of table
 * @param indexName name of index
 * @return int the number of the record meet requirements(-1 represent error)
 */
// int RecordManager::indexRecordAllAlreadyInsert(string tableName,string indexName)
// {
//     fileNode *ftmp = global_buffer->getFile(tableFileNameGet(tableName).c_str());
//     blockNode *btmp = global_buffer->getBlockHead(ftmp);
//     int count = 0;
//     while (true)
//     {
//         if (btmp == NULL)
//         {
//             return -1;
//         }
//         if (btmp->ifbottom)
//         {
//             int recordBlockNum = indexRecordBlockAlreadyInsert(tableName, indexName, btmp);
//             count += recordBlockNum;
//             return count;
//         }
//         else
//         {
//             int recordBlockNum = indexRecordBlockAlreadyInsert(tableName, indexName, btmp);
//             count += recordBlockNum;
//             btmp = global_buffer->getNextBlock(ftmp, btmp);
//         }
//     }
    
//     return -1;
// }


/**
 *
 * insert the index of a record of a table in a block
 * @param tableName name of table
 * @param indexName name of index
 * @param block search record in the block
 * @return int the number of the record meet requirements in the block(-1 represent error)
 */
//  int RecordManager::indexRecordBlockAlreadyInsert(string tableName,string indexName,  blockNode* block)
// {
//     //if block is null, return -1
//     if (block == NULL)
//     {
//         return -1;
//     }
//     int count = 0;
    
//     char* recordBegin = global_buffer->getContent(*block);
// //    vector<Attribute> attributeVector;
//     int recordSize = RECORD_SIZE;
    
// //    api->attributeGet(tableName, &attributeVector);
    
//     int type;
//     int typeSize;
//     char * contentBegin;
    
//     while (recordBegin - global_buffer->getContent(*block)  < global_buffer->getUsedSize(*block))
//     {
//         contentBegin = recordBegin;
//         //if the recordBegin point to a record
//         for (int i = 0; i < attributeVector.size(); i++)
//         {
//             type = attributeVector[i].type;
//             typeSize = TYPE_SIZE;
            
//             //find the index  of the record, and insert it to index tree
//             if (attributeVector[i].index == indexName)
//             {
//                 //api->indexInsert(indexName, contentBegin, type, block->offsetNum);
//                 count++;
//             }
            
//             contentBegin += typeSize;
//         }
//         recordBegin += recordSize;
//     }
    
//     return count;
// }

/**
 *
 * judge if the record meet the requirement
 * @param recordBegin point to a record
 * @param recordSize size of the record
 * attributeVector the attribute list of the record
 * @param conditionVector the conditions
 * @return bool if the record fit the condition
 */
bool RecordManager::recordConditionFit(char* recordBegin,int recordSize, vector<Condition>* conditionVector)
{
    if (conditionVector == NULL) {
        return true;
    }
    // int type;
    // string attributeName;
    int typeSize=TYPE_SIZE;
    char content[TYPE_SIZE];
	char *cont=content;
    char *contentBegin = recordBegin;
	Condition* con = &(*conditionVector)[0];

    if((*conditionVector).size()==1)
    {
        contentBegin=recordBegin+((*conditionVector)[0].attributeIndex)*TYPE_SIZE;

        memset(content, ' ',TYPE_SIZE);
        memcpy(content, contentBegin,TYPE_SIZE);
		content[TYPE_SIZE - 1] = '\0';
		cont = content;
        if(!contentConditionFit(cont,con))
        {
            //if this record is not fit the conditon
            return false;
        }
        return true;
    }
    else if((*conditionVector).size()==2)
    {
        contentBegin=recordBegin+((*conditionVector)[0].attributeIndex)*TYPE_SIZE;

        memset(content, ' ',TYPE_SIZE);
        memcpy(content, contentBegin, typeSize);
		content[TYPE_SIZE - 1] = '\0';
		cont = content;

        if(!contentConditionFit(cont, con))
        {
            //if this record is not fit the conditon
            return false;
        }

        contentBegin=recordBegin+((*conditionVector)[1].attributeIndex)*TYPE_SIZE;

        memset(content, ' ',TYPE_SIZE);
        memcpy(content, contentBegin, typeSize);
		content[TYPE_SIZE - 1] = '\0';
		cont = content;

		con = &(*conditionVector)[1];

        if(!contentConditionFit(cont, con))
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

    // for(int i = 0; i < (*conditionVector).size(); i++)
    // {
    //     //type = (*attributeVector)[i].type;
    //     //attributeName = (*attributeVector)[i].name;
    //     //typeSize = TYPE_SIZE;
        
    //     //init content (when content is string , we can get a string easily)
    //     contentBegin=recordBegin+((*conditionVector)[i].attributeIndex-1)*TYPE_SIZE;

    //     memset(content, 0,TYPE_SIZE);
    //     memcpy(content, contentBegin, typeSize);

    //     if(!contentConditionFit(content, &(*conditionVector)[i]))
    //     {
    //         //if this record is not fit the conditon
    //         return false;
    //     }


    //     // for(int j = 0; j < (*conditionVector).size(); j++)
    //     // {
    //     //     if ((*conditionVector)[j].attributeName == attributeName)
    //     //     {
    //     //         //if this attribute need to deal about the condition
    //     //         if(!contentConditionFit(content, type, &(*conditionVector)[j]))
    //     //         {
    //     //             //if this record is not fit the conditon
    //     //             return false;
    //     //         }
    //     //     }
    //     // }

    //     //contentBegin += typeSize;
    // }
    // return true;
}

/**
 *
 * print value of record
 * @param recordBegin point to a record
 * @param recordSize size of the record

 */
bool RecordManager::recordPrint(char* recordBegin, int recordSize)
{
    char content[RECORD_SIZE];
    memset(content, ' ', RECORD_SIZE);
    memcpy(content, recordBegin, RECORD_SIZE);
	content[RECORD_SIZE - 1] = '\0';
	
    cout<<content<<endl;

    // int type;
    // string attributeName;
    // int typeSize;
    // char content[RECORD_SIZE];
    
    // char *contentBegin = recordBegin;
    // for(int i = 0; i < attributeVector->size(); i++)
    // {
    //     type = (*attributeVector)[i].type;
    //     typeSize = TYPE_SIZE;
        
    //     //init content (when content is string , we can get a string easily)
    //     memset(content, " ", TYPE_SIZE);
        
    //     memset(content, " ", TYPE_SIZE);

    //     for(int j = 0; j < (*attributeNameVector).size(); j++)
    //     {
    //         if ((*attributeNameVector)[j] == (*attributeVector)[i].name)
    //         {
    //             contentPrint(content, type);
    //             break;
    //         }
    //     }
        
    //     contentBegin += typeSize;
    // }
    return true;
}

/**
 *
 * print value of content
 * @param content point to content
 * @param type type of content
 */
// bool RecordManager::contentPrint(char * content, int type)
// {
//     if (type == Attribute::TYPE_INT)
//     {
//         //if the content is a int
//         int tmp = *((int *) content);   //get content value by point
//         printf("%d ", tmp);
//     }
//     else if (type == Attribute::TYPE_FLOAT)
//     {
//         //if the content is a float
//         float tmp = *((float *) content);   //get content value by point
//         printf("%f ", tmp);
//     }
//     else
//     {
//         //if the content is a string
//         string tmp = content;
//         printf("%s ", tmp.c_str());
//     }
//     return true;
// }

/**
 *
 * judge if the content meet the requirement
 * @param content point to content
 *  type type of content
 * @param condition condition
 * @return bool the content if meet
 */
bool RecordManager::contentConditionFit(char* content, Condition* condition)
{
    int type=condition->operationType;
	char value[TYPE_SIZE];
	memset(value, ' ', TYPE_SIZE);
	value[TYPE_SIZE - 1] = '\0';

	const char* p = condition->cmpValue.data();
	memcpy(value, p, condition->cmpValue.size());
//    int res = strcmp(value, content);

    if(type==2)
    {
        if(strcmp(value,content)==0)
            return true;
        else return false;
    }
    if(type==5)
    {
        if(strcmp(value,content)!=0)
            return true;
        else return false;
    }

    const char *contentstr=content;
	double contentdou=atof(contentstr);

    const char *valuestr=value;
	double valuedou=atof(valuestr);

    switch(type)
    {
        case 1:
            if(contentdou>valuedou)
                return true;
			return false;
        case 0:
            if(contentdou<valuedou)
                return true;
			return false;
        case 3:
            if(contentdou>=valuedou)
                return true;
			return false;
        case 4:
            if(contentdou<=valuedou)
                return true;
			return false;
        default:
            return false;
    }
    // if (type == Attribute::TYPE_INT)
    // {
    //     //if the content is a int
    //     int tmp = *((int *) content);   //get content value by point
    //     return condition->ifRight(tmp);
    // }
    // else if (type == Attribute::TYPE_FLOAT)
    // {
    //     //if the content is a float
    //     float tmp = *((float *) content);   //get content value by point
    //     return condition->ifRight(tmp);
    // }
    // else
    // {
    //     //if the content is a string
    //     return condition->ifRight(content);
    // }
    // return true;
}

/**
 *
 * get a index's file name
 * @param indexName name of index
 */
string RecordManager::indexFileNameGet(string indexName)
{
    string tmp = "";
    return "INDEX_FILE_"+indexName;
}

/**
 *
 * get a table's file name
 * @param tableName name of table
 */
string RecordManager::tableFileNameGet(string tableName)
{
    string tmp = "";
    return tmp + "TABLE_FILE_" + tableName;
}
