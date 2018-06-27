//
//  API.cpp
//  Interpreter
//
//  Created by Leaf on 2018/6/18.
//  Copyright 漏 2018骞� Leaf. All rights reserved.
//

#include <stdio.h>
#include <sstream>
#include "API.h"

API::API() {
	global_buffer = new Buffer();
	vector <string> allIndex;
	allIndex = cm.getAllIndex();
	vector <string> allIndexType;
	allIndexType = cm.getAllIndexType();

	int i = (int)allIndex.size();
	vector <int> keyType;
	for (int k = 0; k < i; k++) {
		keyType.push_back(this->getKeyType(allIndexType[k]));
	}
	for (int j = 0; j < i; j++) {
		bool a = im.GetIndex(allIndex[j], keyType[j]);
	}
}

// Check if the characters are legal.
bool API::checkLegal(string str) {
	return str.find(";") == -1;
}

// Check if the characters makes a integer less than 255: true if okay and false if not okay.
bool API::checkInteger(string str) {
	int num;
	try {
		num = stoi(str);
	}
	catch (invalid_argument&) {
		return false;
	}
	catch (out_of_range&) {
		cout << "Error: out of range." << endl;
		return false;
	}
	catch (...) {
		return false;
	}
	return num>0 && num<255;
}

// Get the index of operator from {=, >, <, >=, <=, <>} or -1 if not matched.
bool API::checkOperator(string opString) {
	if (opString.length() == 1) {
		return opString[0] == '=' || opString[0] == '>' || opString[0] == '<';
	}
	else {
		return opString.compare(">=") == 0 || opString.compare("<=") == 0 || opString.compare("<>") == 0;
	}
}

bool API::checkUnique(string tableName, int indexOfAttribute, string value)
{
	vector <Condition> conditionVector;
	Condition condition(indexOfAttribute, "=", value);
	conditionVector.push_back(condition);
	int i;
	i = rm.recordAllFind(tableName + ".db", &conditionVector);
	if (i == 0) {
		return true;//鏄敮涓€鐨�
	}
	else {
		return false;//涓嶅敮涓€
	}
}

bool API::select(string tableName, vector<Condition> conditionVec)
{
	if (conditionVec.size() > 1) { //澶氭煡鎵炬潯浠�
		int suc = rm.recordAllShow(tableName + ".db", &conditionVec);
		if (suc != -1) {
			return true;
		}
		else {
			return false;
		}
	}
	if (conditionVec.size() == 1) { //鍗曟煡鎵炬潯浠�
		int i = conditionVec[0].attributeIndex;//鏄〃鐨勭鍑犱釜灞炴€�
		vector<string> attributeName = cm.getAttributeName(tableName);//鑾峰緱琛ㄧ殑鎵€鏈夊睘鎬�
		string indexName = cm.getIndexName(tableName, attributeName[i]);
		if (indexName == "") { //鍗曟煡鎵炬潯浠舵病鏈夌储寮曠殑鎯呭喌
			int suc = rm.recordAllShow(tableName + ".db", &conditionVec);
			if (suc != -1) {
				return true;
			}
			else {
				return false;
			}
		}
		else { //鍗曟煡鎵炬潯浠舵湁绱㈠紩鐨勬儏鍐�
			int KeyType = getKeyType((cm.getAttributeScheme(tableName))[conditionVec[0].attributeIndex]);
			int offSet = im.SearchInIndex(indexName + "_index.db", conditionVec[0].cmpValue, KeyType);
			int suc = rm.recordBlockShow(tableName + ".db", &conditionVec, offSet);
			if (suc != -1) {
				return true;
			}
			else {
				return false;
			}
		}
	}
	else { //鏃犳煡鎵炬潯浠�
		int suc = rm.recordAllShow(tableName + ".db", &conditionVec);
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
	int offSet = rm.recordInsert(tableName + ".db", valueVec);
	if (offSet == -1) {
		return false;
	}
	vector<string> attributeName = cm.getAttributeName(tableName);
	for (int i = 0; i < attributeName.size(); i++) {
		string indexName = cm.getIndexName(tableName, attributeName[i]);
		if (indexName != "") { //琛ㄧず璇ュ睘鎬ф湁绱㈠紩
			int keyType = getKeyType((cm.getAttributeScheme(tableName))[i]);
			bool suc = im.InsertIntoIndex(indexName + "_index.db", valueVec[i], keyType, offSet); //缁欐湁绱㈠紩鐨勫睘鎬у搴旂殑绱㈠紩杩涜鏇存敼
			if (!suc) {
				return false;
			}
		}
	}
	return true;
}

//////////////////////////////////////////////////////////////////
int API::delete_(string tableName, vector<Condition> conditionVec)
{
	//    int conditionCount = (int)conditionVec.size();
	//    vector<string> attrNames = cm.getAttributeName(tableName);
	//    int attrCount = (int)attrNames.size();
	//    vector<string> indexNames;
	//    string indexName;
	//    int keyType;
	//
	//    for(int i=0;i<attrCount;i++) {
	//        string indexName = cm.getIndexName(tableName, attrNames[i]);
	//        if(indexName != "") {
	//            keyType = getKeyType((cm.getAttributeScheme(tableName))[i]);
	//        }
	//    }
	//
	//    if(!conditionCount) {
	//
	//    }
	//int KeyType = 0;
	//if(conditionVec.size())
	//KeyType = getKeyType((cm.getAttributeScheme(tableName))[conditionVec[0].attributeIndex]);
	//int i = conditionVec[0].attributeIndex;//是表的第几个属性

	// How many records have been deleted.
	int KeyType;
	int deleteCount = 0;

	vector<string> attributeName = cm.getAttributeName(tableName);//获得表的所有属性
																  //string indexName = cm.getIndexName(tableName, attributeName[i]);
	vector <RetRecord> allRecord;


	allRecord = rm.returnRecord(tableName + ".db", &conditionVec); //all record in table
	int attributeNum = cm.getAttributeNum(tableName);//获得有多少个属性值
													 //int position = cm.getAttributePosition(tableName, attributeName);

	if (conditionVec.size() > 1) { //多查找条件
		deleteCount = rm.recordAllDelete(tableName + ".db", &conditionVec);

		for (int j = 0; j < allRecord.size(); j++) {
			for (int k = 0; k < attributeNum; k++) {
				string indexName = cm.getIndexName(tableName, attributeName[k]);
				if (indexName != "") {
					//如果该属性有索引
					KeyType = getKeyType((cm.getAttributeScheme(tableName))[k]);
					im.DeleteFromIndex(indexName + "_index.db", allRecord[j].singleRecord[k], KeyType);
				}
			}
		}
		//if(deleteCount > 0 && im.DeleteFromIndex(indexName + "_index.db", conditionVec[0].cmpValue, KeyType) == false)
		//return -1;
		return deleteCount;
	}
	else if (conditionVec.size() == 1) { //单查找条件
		string attri = (cm.getAttributeName(tableName))[conditionVec[0].attributeIndex]; //找到where后的属性对应的属性名
		string thisIndexName = cm.getIndexName(tableName, attri);
		if (thisIndexName == "") { //单查找条件没有索引的情况
			deleteCount = rm.recordAllDelete(tableName + ".db", &conditionVec);

			for (int j = 0; j < allRecord.size(); j++) {
				for (int k = 0; k < attributeNum; k++) {
					string indexName = cm.getIndexName(tableName, attributeName[k]);
					if (indexName != "") {
						//如果该属性有索引
						KeyType = getKeyType((cm.getAttributeScheme(tableName))[k]);
						im.DeleteFromIndex(indexName + "_index.db", allRecord[j].singleRecord[k], KeyType);
					}
				}
			}

			//if(deleteCount > 0 && im.DeleteFromIndex(indexName + "_index.db", conditionVec[0].cmpValue, KeyType) == false)
			//return -1;

			return deleteCount;
		}
		else { //单查找条件有索引的情况
			int offSet = im.SearchInIndex(thisIndexName + "_index.db", conditionVec[0].cmpValue, KeyType);
			deleteCount = rm.recordBlockDelete(tableName + ".db", &conditionVec, offSet);

			for (int j = 0; j < allRecord.size(); j++) {
				for (int k = 0; k < attributeNum; k++) {
					string indexName = cm.getIndexName(tableName, attributeName[k]);

					if (indexName != "") {
						//如果该属性有索引
						KeyType = getKeyType((cm.getAttributeScheme(tableName))[k]);
						im.DeleteFromIndex(indexName + "_index.db", allRecord[j].singleRecord[k], KeyType);
					}
				}
			}

			//if(deleteCount > 0 && im.DeleteFromIndex(indexName+"_index.db", conditionVec[0].cmpValue, KeyType) == false)
			//       return -1;
			return deleteCount;
		}
	}
	else { //无查找条件
		deleteCount = rm.recordAllDelete(tableName + ".db", &conditionVec);

		//for (int j = 0; j < allRecord.size(); j++) {
		//	for (int k = 0; k < attributeNum; k++) {
		//		if (cm.getIndexName(tableName, attributeName[k]) != "") {
		//			//如果该属性有索引
		//			KeyType = getKeyType((cm.getAttributeScheme(tableName))[k]);
		//			im.DeleteFromIndex(indexName + "_index.db", allRecord[j].singleRecord[k], KeyType);
		//		}
		//	}
		//}
		for (int j = 0; j < allRecord.size(); j++) {
			for (int k = 0; k < attributeNum; k++) {
				string indexName = cm.getIndexName(tableName, attributeName[k]);
				if (indexName != "") {
					//如果该属性有索引
					KeyType = getKeyType((cm.getAttributeScheme(tableName))[k]);
					im.DeleteFromIndex(indexName + "_index.db", allRecord[j].singleRecord[k], KeyType);
				}
			}
		}

		// if(deleteCount > 0 && im.DeleteFromIndex(indexName + "_index.db", conditionVec[0].cmpValue, KeyType) == false)
		//   return -1;
		return deleteCount;
	}
}

bool API::dropTable(string tableName)
{
	vector <string> attributeName = cm.getAttributeName(tableName); //鑾峰緱璇ヨ〃鐨勬墍鏈夊睘鎬�
	for (int i = 0; i < attributeName.size(); i++) {
		string indexName = cm.getIndexName(tableName, attributeName[i]); //渚濇妫€鏌ユ墍鏈夊睘鎬ф槸鍚︽湁绱㈠紩
		if (indexName != "") {
			bool suc = this->dropIndex(indexName); //鍒犳帀鏈夌储寮曠殑灞炴€х殑绱㈠紩
			if (!suc) {
				return false;
			}
		}
	}
	global_buffer->deleteFileNode(tableName + ".db");
	return cm.dropTable(tableName); //鍒犳帀琛ㄦ枃浠�
}

bool API::createIndex(string tableName, string attributeName, string indexName)
{
	int i = cm.getAttributePosition(tableName, attributeName); //鑾峰緱璇ュ睘鎬у悕鍦ㄨ琛ㄧ殑灞炴€у垪琛ㄤ腑鐨勪綅缃紙鍗砳ndex
	string attributeType = (cm.getAttributeScheme(tableName))[i];
	int keyType = getKeyType(attributeType); //鑾峰緱璇ュ睘鎬х殑绫诲瀷
	int keySize = 0;
	if (keyType == 0) {
		keySize = 4;
	}
	else if (keyType == 1) {
		keySize = 4;
	}
	else {
		string sub = attributeType.substr(4);
		keySize = stoi(sub);
	}
	bool succ = cm.createIndex(tableName, attributeName, indexName); //鍦╟atalogManager涓褰曠浉鍏充俊鎭�
	bool suc = im.CreateIndex(indexName + "_index.db", keySize, keyType); //鍦╥ndexManager涓垱寤築+鏍�
	im.GetIndex(indexName + "_index.db", keyType);
	vector <RetRecord> allRecord;
	vector <Condition> conditionVec;

	allRecord = rm.returnRecord(tableName + ".db", &conditionVec); //all record in table
	int position = cm.getAttributePosition(tableName, attributeName);

	int k = cm.getAttributePosition(tableName, attributeName);
	attributeType = (cm.getAttributeScheme(tableName))[k];
	keyType = getKeyType(attributeType);

	for (int i = 0; i < allRecord.size(); i++) {
		string KeyValue = allRecord[i].singleRecord[position];
		int Offset = allRecord[i].offsetNum;
		suc = im.InsertIntoIndex(indexName + "_index.db", KeyValue, keyType, Offset);
		if (!suc) {
			return false;
		}
	}
	return suc && succ;
}

bool API::createTable(string tableName, int primKeyIndex, vector<string> attrName, vector<string> attrType, vector<int>uniqIndex)
{
	vector<int> unique;
	int j = 0;
	for (int i = 0; i < attrName.size(); i++) {
		if (j < uniqIndex.size() && i == uniqIndex[j]) {
			unique.push_back(1);
			j++;
		}
		else {
			unique.push_back(0);
		}
	}
	bool suc = cm.createTable(tableName, attrType, attrName, attrName[primKeyIndex], unique); //鍒涘缓琛�
	bool succ = this->createIndex(tableName, attrName[primKeyIndex], attrName[primKeyIndex]); //缁欎富閿缓绔嬬储寮曪紝绱㈠紩鍚嶄负priIndex
	return suc && succ;
}

//bool API::createTable(string tableName, int primKeyIndex, vector<string> attrName, vector<string> attrType, vector<int>uniqIndex)//ok
//{
//    vector<int> unique;
//    int j = 0;
//    for (int i = 0; i < attrName.size(); i++) {
//        if (i == uniqIndex[j]) {
//            unique.push_back(1);
//            j++;
//        }
//        else {
//            unique.push_back(0);
//        }
//    }
//    bool suc = cm.createTable(tableName, attrType, attrName, attrName[primKeyIndex], unique); //鍒涘缓琛�
//    bool succ = this->createIndex(tableName, attrName[primKeyIndex], "priIndex"); //缁欎富閿缓绔嬬储寮曪紝绱㈠紩鍚嶄负priIndex
//    return suc && succ;
//}
