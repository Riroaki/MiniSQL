//
//  API.h
//  Interpreter
//
//  Created by Leaf on 2018/6/18.
//  Copyright © 2018年 Leaf. All rights reserved.
//

#ifndef API_h
#define API_h

#include <vector>
#include <string>
#include <iostream>
#include "CatalogManager.h"
#include "Record_Manager.h"
#include "IndexManager.h"
#include "condition.h"

using namespace std;
extern Buffer* global_buffer;

class API {
private:
    CatalogManager cm;
    RecordManager rm;
	IndexManager im;
	//将catalog中储存的属性类型（int、float……）转换成indexManager里的0，1，2。
	//ok
	int getKeyType(string attributeType) 
	{
		if (attributeType == "int") {
			return 0;
		}
		if (attributeType == "float") {
			return 1;
		}
		else {
			return 2;
		}
	}
	
public:
    
    // Initialize the API and the buffer.
    API();
    
    // The executors:   
    // TODO:
    // Excecute the select operation, returning the results in a 2-d vector.
	bool select(string tableName, vector<Condition> conditionVec); //ok
    
    // Execute the insert operation, returning insertion status: true if ok and false if errors occur.
	bool insert(string tableName, vector<string> valueVec); //ok

    // Execute the delete operation, returning delete result: number of deleted entries and 0 if nothing happens.
	int delete_(string tableName, vector<Condition> conditionVec); //ok
    
    // Execute the drop table operation, returning drop status: true if okay and false if errors occur.
	bool dropTable(string tableName);//ok
    
    // Execute the drop index operation, returning drop status: true if okay and false if errors occur.
	//ok
    bool dropIndex(string indexName) //ok
    {
		bool i = im.DeleteIndex(indexName+"_index.db");
		global_buffer->deleteFileNode(indexName + "_index.db");
		bool j = cm.dropIndex(indexName);
		return i && j;
	}
    
    // Execute the create index operation, returning create status: true if ok and false if errors occur.
	//即将ok
	bool createIndex(string tableName, string attributeName, string indexName);
    
    // Execute the create table operation, returing create status: true if ok and false if errors occur.
	//ok
	bool createTable(string tableName, int primKeyIndex, vector<string> attrName, vector<string> attrType, vector<int>uniqIndex);
    
    // The checks & gets & finds:
    
    
    // DONE:
    // Check if the characters are legal.
	bool checkLegal(string str);//ok
    
    // Check if the characters make a integer: true if okay and false if not okay.
    bool checkInteger(string str);//ok
    
    // Get the index of operator from {=, >, <, >=, <=, <>} or -1 if not matched.
    bool checkOperator(string opString);//ok
    
    // TODO:
    // Check the uniqueness of one attribute in the table given name.
	bool checkUnique(string tableName, int indexOfAttribute, string value);
    
    // Check if a table exists, return true if exist and false if not.
	//ok
    bool checkTable(string tableName)
	{
		return cm.checkFile(tableName+".db");
	}
    
    // Check if an index exists, return true if exist and false if not.
    bool checkIndex(string indexName) //ok
	{
		return cm.checkIndex(indexName);
	}
    
    // Check the format of inserted tuple in the table with given name.
    bool checkInsertFormat(string tableName, vector<string> valueVec) //ok
	{
		return cm.judgeAttribute(tableName, valueVec);
	}
    
    // Get the attributes' types of the table with given table name.
    vector<string> getAttributeTypes(string tableName) //ok
	{
		return cm.getAttributeScheme(tableName);
	}
    
    // Get the attributes' names of the table with given table nams.
    vector <string> getAttributeNames(string tableName)
    {
        return cm.getAttributeName(tableName);
    }
    
    // Get the index of an attribute in a table, or return -1 if not found.
    int findAttribute(string tableName, string attributeName) //ok
	{
		return cm.getAttributePosition(tableName, attributeName);
	}
    
    // Get the index of unique keys and primary key of the table given name.
    vector<int> getUniqueIndices(string tableName) //ok
	{
		return cm.getUnique(tableName);
	}
    
};

#endif /* API_h */
