//
//  API.cpp
//  Interpreter
//
//  Created by Leaf on 2018/6/18.
//  Copyright © 2018年 Leaf. All rights reserved.
//

#include <stdio.h>
#include <sstream>
#include "API.h"

// Check if the characters are legal.
bool API::checkLegal(string str){
    return str.find(";") == -1;
}

// Check if the characters makes a integer less than 255: true if okay and false if not okay.
bool API::checkInteger(string str){
    int num;
    try {
        num = stoi(str);
    } catch(invalid_argument&) {
        return false;
    } catch(out_of_range&) {
        cout<<"Error: out of range."<<endl;
        return false;
    } catch(...) {
        return false;
    }
    return num < 255 && num > 0;
}

// Get the index of operator from {=, >, <, >=, <=, <>} or -1 if not matched.
bool API::checkOperator(string opString) {
    if(opString.length() == 1) {
        return opString[0] == '=' || opString[0] == '>' || opString[0] == '<';
    } else {
        return opString.compare(">=") == 0 || opString.compare("<=") == 0 || opString.compare("<>") == 0;
    }
}

bool API::checkUnique(string tableName, int indexOfAttribute, string value) 
{
//    vector<int> unique;
	//unique = cm.getUnique(tableName);
    return true;
	
}

bool API::select(string tableName, vector<Condition> conditionVec)
{
	vector<Condition> *conditionVector = &conditionVec;
	if (conditionVec.size() > 1) { //多查找条件
		int suc = rm.recordAllShow(tableName, conditionVector);
		if (suc != -1) {
			return true;
		}
		else {
			return false;
		}
	}
	if (conditionVec.size() == 1) { //单查找条件
		int i = conditionVec[0].attributeIndex;//是表的第几个属性
		vector<string> attributeName = cm.getAttributeName(tableName);//获得表的所有属性
		string indexName = cm.getIndexName(tableName, attributeName[i]);
		if (indexName == "") { //单查找条件没有索引的情况
			int suc = rm.recordAllShow(tableName, conditionVector);
			if (suc != -1) {
				return true;
			}
			else {
				return false;
			}
		}
		else { //单查找条件有索引的情况
			int KeyType = getKeyType((cm.getAttributeScheme(tableName))[conditionVec[0].attributeIndex]);
			int offSet = im.SearchInIndex(indexName, conditionVec[0].cmpValue, KeyType);
			int suc = rm.recordBlockShow(tableName, conditionVector, offSet);
			if (suc != -1) {
				return true;
			}
			else {
				return false;
			}
		}
	}
	else { //无查找条件
		int suc = rm.recordAllShow(tableName, conditionVector);
		if (suc != -1) {
			return true;
		}
		else {
			return false;
		}
	}
}

bool API::insert(string tableName, vector<string> valueVec)
{
	vector<string>* record = &valueVec;
	int offSet = rm.recordInsert(tableName, record);
	if (offSet == -1) {
		return false;
	}
	vector<string> attributeName = cm.getAttributeName(tableName);
	for (int i = 0; i < attributeName.size(); i++) {
		string indexName = cm.getIndexName(tableName, attributeName[i]);
		if (indexName != "") { //表示该属性有索引
			int keyType = getKeyType((cm.getAttributeScheme(tableName))[i]);
			bool suc = im.InsertIntoIndex(indexName, valueVec[i], keyType, offSet); //给有索引的属性对应的索引进行更改
			if (!suc) {
				return false;
			}
		}
	}
	return true;
}

int API::delete_(string tableName, vector<Condition> conditionVec)
{
	vector<Condition> *conditionVector;
	(*conditionVector) = conditionVec;
	if (conditionVec.size() > 1) { //多查找条件
		int suc = rm.recordAllDelete(tableName, conditionVector);
		if (suc != -1) {
			return true;
		}
		else {
			return false;
		}
	}
	if (conditionVec.size() == 1) { //单查找条件
		int i = conditionVec[0].attributeIndex;//是表的第几个属性
		vector<string> attributeName = cm.getAttributeName(tableName);//获得表的所有属性
		string indexName = cm.getIndexName(tableName, attributeName[i]);
		if (indexName == "") { //单查找条件没有索引的情况
			int suc = rm.recordAllDelete(tableName, conditionVector);
			if (suc != -1) {
				return true;
			}
			else {
				return false;
			}
		}
		else { //单查找条件有索引的情况
			int KeyType = getKeyType((cm.getAttributeScheme(tableName))[conditionVec[0].attributeIndex]);
			int offSet = im.SearchInIndex(indexName, conditionVec[0].cmpValue, KeyType);
			int suc = rm.recordBlockDelete(tableName, conditionVector, offSet);
			bool succ = im.DeleteFromIndex(indexName, conditionVec[0].cmpValue, KeyType);
			if (suc != -1 && succ == true) {
				return true;
			}
			else {
				return false;
			}
		}
	}
	else { //无查找条件
		int suc = rm.recordAllShow(tableName, conditionVector);
		if (suc != -1) {
			return true;
		}
		else {
			return false;
		}
	}
}

bool API::dropTable(string tableName)
{
	vector <string> attributeName = cm.getAttributeName(tableName); //获得该表的所有属性
	for (int i = 0; i < attributeName.size(); i++) {
		string indexName = cm.getIndexName(tableName, attributeName[i]); //依次检查所有属性是否有索引
		if (indexName != "") {
			bool suc = this->dropIndex(indexName); //删掉有索引的属性的索引
			if (!suc) {
				return false;
			}
		}
	}
	return cm.dropTable(tableName); //删掉表文件
}

bool API::createIndex(string tableName, string attributeName, string indexName)
{
	int i = cm.getAttributePosition(tableName, attributeName); //获得该属性名在该表的属性列表中的位置（即index
	string attributeType = (cm.getAttributeScheme(tableName))[i];
	int keyType = getKeyType(attributeType); //获得该属性的类型
	int keySize = 0;
	if (keyType == 0) {
		keySize = 4;
	}
	else if (keyType == 1) {
		keySize = 8;
	}
	else {
		string sub = attributeType.substr(4);
		keySize = stof(sub);
	}
	bool suc = im.CreateIndex(indexName, keySize, keyType); //在indexManager中创建B+树
	bool succ = cm.createIndex(tableName, attributeName, indexName); //在catalogManager中记录相关信息
	return suc && succ;
}

bool API::createTable(string tableName, int primKeyIndex, vector<string> attrName, vector<string> attrType, vector<int>uniqIndex)//ok
{
	vector<int> unique;
	int j = 0;
	for (int i = 0; i < attrName.size(); i++) {
		if (i == uniqIndex[j]) {
			unique.push_back(1);
			j++;
		}
		else {
			unique.push_back(0);
		}
	}
	bool suc = cm.createTable(tableName, attrType, attrName, attrName[primKeyIndex], unique); //创建表
	bool succ = this->createIndex(tableName, attrName[primKeyIndex], "priIndex"); //给主键建立索引，索引名为priIndex
	return suc && succ;
}
