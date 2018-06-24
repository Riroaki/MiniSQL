#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H
#include "condition.h"
#include "Record_Manager.h"
#include "BufferManager.h"
#include <string>
#include <vector>


using namespace std;

class RecordManager{
    public:
        RecordManager(){}
        Buffer bm;
        //API *api;

		int tableCreate(string tableName);
        int tableDrop(string tableName);

    //    int indexCreate(string indexName);
    //    int indexDrop(string indexName);

		int recordInsert(string tableName, vector<string>* record);
        
        int recordAllShow(string tableName, vector<Condition>* conditionVector);
        int recordBlockShow(string tableName, vector<Condition>* conditionVector, int blockOffset);
        
        int recordAllFind(string tableName, vector<Condition>* conditionVector);
        
        int recordAllDelete(string tableName, vector<Condition>* conditionVector);
        int recordBlockDelete(string tableName,  vector<Condition>* conditionVector, int blockOffset);
        
    //    int indexRecordAllAlreadyInsert(string tableName,string indexName);
        
        string tableFileNameGet(string tableName);
        string indexFileNameGet(string indexName);
    private:
        int recordBlockShow(string tableName, vector<Condition>* conditionVector, blockNode* block);
        int recordBlockFind(string tableName, vector<Condition>* conditionVector, blockNode* block);
        int recordBlockDelete(string tableName,  vector<Condition>* conditionVector, blockNode* block);
    //    int indexRecordBlockAlreadyInsert(string tableName,string indexName, blockNode* block);
        
        bool recordConditionFit(char* recordBegin,int recordSize,vector<Condition>* conditionVector);
        bool recordPrint(char* recordBegin, int recordSize);
        bool contentConditionFit(char* content,Condition* condition);
    //    bool contentPrint(char * content, int type);
};

#endif    
