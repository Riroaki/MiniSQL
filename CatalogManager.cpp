#include "CatalogManager.h"
#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <stdio.h>
#include <sstream>

using namespace std;

bool CatalogManager::checkType(string value, string type)
{
    if(type == "int") {// int
        int num;
        try {
            num = stoi(value);
        } catch (invalid_argument&) {
            return false;
        } catch (out_of_range&) {
            cout<<"Out of range error!"<<endl;
            return false;
        } catch (...) {
            return false;
        }
    } else if(type == "float") {// float
        float num;
        try {
            num = stof(value);
        } catch (invalid_argument&) {
            return false;
        } catch (out_of_range&) {
            cout<<"Out of range error!"<<endl;
            return false;
        } catch (...) {
            return false;
        }
    } else {// char(xx)
        int trueLen = (int)value.length();
        int len = stoi(type.substr(4));
        return value[0] == '\'' && value[trueLen-1] == '\'' && trueLen-2 <= len;
	}
	return true;
}

bool CatalogManager::createTable(string tableName, vector<string> attributeScheme, vector<string> attributeName, string primaryKey, vector<int>unique)
{
	ofstream catalog;
	string path = tableName + ".db";
	if (this->checkFile(path)) {
		cout << "The table has been created!" << endl;
		return false;
	}
	catalog.open(path.c_str());
	catalog.close();


	path = getFilePath(tableName);
	if (this->checkFile(path)) {
		cout << "The table has been created!" << endl;
		return false;
	}
	catalog.open(path.c_str());

	for (int i = 0; i < attributeScheme.size(); i++) {
		int pri_key = 0;
		if (attributeName[i] == primaryKey.c_str()) {
			pri_key = 1;
		}
		while (attributeScheme[i].length() < ATTR_SCHEME_LENGTH) {

			attributeScheme[i] = attributeScheme[i] + " ";
		}
		catalog << attributeScheme[i].c_str();
		while (attributeName[i].length() < ATTR_NAME_LENGTH) {
			attributeName[i] = attributeName[i] + " ";
		}
		catalog << attributeName[i].c_str();
		if (pri_key == 1) {
			catalog << "1";
		}
		else {
			catalog << "0";
		}
		if (unique[i] == 1) {
			catalog << "1";
		}
		else {
			catalog << "0";
		}
		if (pri_key == 1) {
			catalog << "1";
		}
		else {
			catalog << "0";
		}
		
	}
	catalog.close();
	return true;
}

bool CatalogManager::dropTable(string tableName) {

	//string path = "db\\";
	//path = path + tableName;
	string path = tableName + ".db";
	if (this->checkFile(path)) {
		remove(path.c_str());
	}
	else {
//        cout << "No such a table!" << endl;
		return false;
	}
	path = getFilePath(tableName);
	if (this->checkFile(path)) {
		remove(path.c_str());
	}
	else {
//        cout << "No such a table!" << endl;
		return false;
	}
	return true;
}

vector<string> CatalogManager::getAttributeScheme(string tableName)
{
	vector <string> attributeScheme;
	string path = getFilePath(tableName);
	if (!this->checkFile(path)) {
//        cout << "No such a table!" << endl;
		return attributeScheme;
	}
	ifstream catalog(path.c_str());
	catalog.seekg(ios::beg);
	int i = 0;
	string scheme;

	while (!catalog.eof()) {
		catalog.seekg(ios::beg);
		catalog.seekg(i * (ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH + PRIMARY_KEY_LENGTH + UNIQUE_LENGTH + INDEX_LENGTH), ios::cur);
		catalog >> scheme;
		attributeScheme.push_back(scheme);
		i++;
	}
	attributeScheme.pop_back();
	return attributeScheme;
}

vector<string> CatalogManager::getAttributeName(string tableName)
{
	vector <string> attributeName;
	string path = getFilePath(tableName);
	if (!this->checkFile(path)) {
//        cout << "No such a table!" << endl;
		return attributeName;
	}
	ifstream catalog(path.c_str());
	catalog.seekg(ios::beg);
	int i = 0;
	string scheme;
	while (!catalog.eof()) {
		catalog.seekg(ios::beg);
		catalog.seekg(i * (ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH + PRIMARY_KEY_LENGTH + UNIQUE_LENGTH + INDEX_LENGTH) + ATTR_SCHEME_LENGTH, ios::cur);
		catalog >> scheme;
		attributeName.push_back(scheme);
		i++;
	}
	attributeName.pop_back();
	return attributeName;
}

int CatalogManager::getAttributeNum(string tableName)
{
	vector <string> attributeScheme;
	attributeScheme = getAttributeScheme(tableName);
	return (int)attributeScheme.size();
}

bool CatalogManager::judgeAttribute(string tableName, vector <string> attributeValue) 
{
	vector <string> attributeScheme;
	attributeScheme = getAttributeScheme(tableName);
	if (attributeValue.size() != attributeScheme.size()) {
		return false;
	}
	for (int i = 0; i < attributeScheme.size(); i++) {
		if (this->checkType(attributeValue[i], attributeScheme[i]) == false) {
			return false;
		}
	}
	return true;
}

vector<int> CatalogManager::getUnique(string tableName)
{
	vector<int> unique;//注：这里的unique与create table里的unique向量含义不相同。
	string path = getFilePath(tableName);
	if (!this->checkFile(path)) {
//        cout << "No such a table!" << endl;
		return unique;
	}
	ifstream catalog(path.c_str());
	catalog.seekg(ios::beg);
	char uni;
	int number = getAttributeNum(tableName);
	for (int i = 0; i < number; i++) {
		catalog.seekg(ios::beg);
		catalog.seekg(i * (ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH + PRIMARY_KEY_LENGTH + 
		UNIQUE_LENGTH + INDEX_LENGTH) + ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
		catalog >> uni;
		if (uni == '1') {
			unique.push_back(i);
		}
		else {
			catalog >> uni;
			if (uni == '1') {
				unique.push_back(i);
			}
		}
	}
	return unique;
}

int CatalogManager::getPrimaryKey(string tableName) 
{
	string path = getFilePath(tableName);
	if (!this->checkFile(path)) {
//        cout << "No such a table!" << endl;
		return 0;
	}
	ifstream catalog(path.c_str());
	catalog.seekg(ios::beg);
	char pri;
	int number = getAttributeNum(tableName);
	for (int i = 0; i < number; i++) {
		catalog.seekg(ios::beg);
		catalog.seekg(i * (ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH + PRIMARY_KEY_LENGTH + UNIQUE_LENGTH + INDEX_LENGTH) + ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
		catalog >> pri;
		if (pri == '1') {
			return i;
		}
	}
	return -1;
}

int CatalogManager::getAttributePosition(string tableName, string name)
{
	vector<string>attributeName;
	attributeName = getAttributeName(tableName);
	for (int i = 0; i < attributeName.size(); i++) {
		if (name == attributeName[i]) {
			return i;
		}
	}
	return -1;
}

bool CatalogManager::createIndex(string tableName, string attributeName, string indexName)
{
    if (checkIndex(indexName)) {
        return false;
    }
    fstream catalog;
    string path = getFilePath(tableName);
    if (!this->checkFile(path)) {
//        cout << "No such a table!" << endl;
        return false;
    }
    path = indexName+"_index.db";
    ofstream index;
    index.open(path);
    index.close();

    path = "db\\index.db";
    if (!this->checkFile(path)) {
        index.open(path.c_str());
        index.close();
    }
    
    index.open(path, ios::app);
    
    
    while (tableName.length() < TABLE_NAME_LENGTH) {
        tableName += " ";
    }
    index << tableName;
    while (attributeName.length() < ATTR_NAME_LENGTH) {
        attributeName += " ";
    }
    index << attributeName;
    while (indexName.length() < INDEX_NAME_LENGTH) {
        indexName += " ";
    }
    
    index << indexName;
    index.close();
    
    return true;
}

bool CatalogManager::dropIndex(string indexName)
{
	fstream index("db\\index.db");
	index.seekg(ios::beg);
	int i = 0;
    //remove(indexName+"_index.db");
    string indexFileName = indexName+"_index.db";
    remove(indexFileName.c_str());
	string thisIndexName;
	while (!index.eof()) {
		index.seekg(ios::beg);
		index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
		index >> thisIndexName;
		if (thisIndexName == indexName) {
			index.seekg(ios::beg);
			index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH), ios::cur);
			string tableName;
			index >> tableName;
			index.seekg(ios::beg);
			index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH, ios::cur);
			string attributeName;
			index >> attributeName;

			index.seekp(ios::beg);
			index.seekp(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH), ios::cur);
			for (int j = 0; j < TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH; j++) {
				index << " ";
			}
			index.close();

			fstream catalog;
			string path = getFilePath(tableName);
			int i = getAttributePosition(tableName, attributeName);

			catalog.open(path, ios::in | ios::out);
			catalog.seekp(ios::beg);
			catalog.seekp(i * (ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH + PRIMARY_KEY_LENGTH + UNIQUE_LENGTH + INDEX_LENGTH) + ATTR_SCHEME_LENGTH + ATTR_NAME_LENGTH + PRIMARY_KEY_LENGTH + UNIQUE_LENGTH, ios::cur);
			catalog << "0";
			catalog.close();
			return true;
		}
		i++;
	}
	return false;
}

string CatalogManager::getIndexName(string tableName, string attributeName)
{
	ifstream index("db\\index.db");
	string thisTableName;
	int i = 0;
	while (!index.eof()) {
		index.seekg(ios::beg);
		index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH), ios::cur);
		index >> thisTableName;
		if (index.eof()) {
			break;
		}
		if (thisTableName == tableName) {
			index.seekg(ios::beg);
			index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH, ios::cur);
			string thisAttributeName;
			index >> thisAttributeName;
			if (thisAttributeName == attributeName) {
				index.seekg(ios::beg);
				index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
				string indexName;
				index >> indexName;
				return indexName;
			}
		}
		i++;
	}
	string indexName = "";
	return  indexName;
}

bool CatalogManager::checkIndex(string indexName)
{
	string path = "db\\index.db";
	if (!this->checkFile(path)) {
		return false;
	}
	ifstream index("db\\index.db");
	int i = 0;
	string thisIndexName;
	while (!index.eof()) {
		index.seekg(ios::beg);
		index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
		index >> thisIndexName;
		if (index.eof()) {
			break;
		}
		if (indexName == thisIndexName) {
			return true;
		}
		i++;
	}
	return false;
}

vector <string> CatalogManager::getAllIndex()
{
    ifstream index("db\\index.db");
    //string thisTableName;
    vector <string> allIndex;
    string indexName;
    int i = 0;
    while (index.peek() != EOF) {
        index.seekg(ios::beg);
        index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
        
        if (index.peek() == ' ') {
            i++;
            continue;
        }
        index.seekg(ios::beg);
        index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH + ATTR_NAME_LENGTH, ios::cur);
        index >> indexName;
        if (index.eof()) {
            break;
        }
        allIndex.push_back(indexName+"_index.db");
        
        i++;
    }
    return  allIndex;
}

vector <string> CatalogManager::getAllIndexType()
{
    ifstream index("db\\index.db");
    //string thisTableName;
    vector <string> allIndexType;
    string indexNameType;
    int i = 0;
    while (index.peek() != EOF) {
        index.seekg(ios::beg);
        index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH, ios::cur);
        
        if (index.peek() == ' ') {
            i++;
            continue;
        }
        index.seekg(ios::beg);
        index.seekg(i * (TABLE_NAME_LENGTH + ATTR_NAME_LENGTH + INDEX_NAME_LENGTH) + TABLE_NAME_LENGTH, ios::cur);
        index >> indexNameType;
        if (index.eof()) {
            break;
        }
        allIndexType.push_back(indexNameType);
        
        i++;
    }
    return  allIndexType;
}
