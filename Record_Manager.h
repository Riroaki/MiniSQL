#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H
//#include "API.h"
#include "Record_Manager.h"
#include "BufferManager.h"
#include "condition.h"
//#include "Index_Manager.h"
//#include "Interpreter.h"
//#include "Minisql.h"
#include <string>
#include <vector>


using namespace std;
//class API;

extern Buffer* global_buffer;


class RetRecord {
public:
	int offsetNum;
	vector<string> singleRecord;
};


class RecordManager{
    public:
        RecordManager(){}
        //API *api;
		vector<RetRecord> RecordManager::returnRecord(string tableName, vector<Condition>* conditionVector);
		vector<RetRecord> RecordManager::recordBlockReturn(string tableName, vector<Condition>* conditionVector, blockNode* block);
		int tableCreate(string tableName);
        int tableDrop(string tableName);

    //    int indexCreate(string indexName);
    //    int indexDrop(string indexName);

		int recordInsert(string tableName, vector<string> record);
        
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
