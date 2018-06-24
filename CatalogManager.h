#ifndef CATALOGMANAGER_H
#define CATALOGMANAGER_H

#define ATTR_SCHEME_LENGTH 8
#define ATTR_NAME_LENGTH 11
#define PRIMARY_KEY_LENGTH 1
#define UNIQUE_LENGTH 1
#define INDEX_LENGTH 1
#define TABLE_NAME_LENGTH 21
#define INDEX_NAME_LENGTH 11

#include <string>
#include <vector>
#include <fstream>
#include <iostream>

using namespace std;

class CatalogManager {
private:
	//检查某一个属性值是否与catalog中的记录的属性类型相吻合。该函数被judgeAttribute函数调用。
	bool checkType(string value, string type);

	
public:
    string getFilePath(string tableName)
    {
        string path = "db\\";
        path = path + tableName;
        path = path + "_attr.db";
        return path;
    }
    
	//给出表名，检查该表是否存在
	bool checkFile(string File) 
	{
		ofstream catalog;
		
		catalog.open(File.c_str(), ios::in);
		if (catalog) {
			return true;
		}
		else {
			return false;
		}
	}
	//创建table（创建了[tablename]_data.txt, [tablename]_attr.txt两个文件），将表格属性的信息存入[tablename]_attr.txt文件中，创建成功返回true，否则返回false
	bool createTable(string tableName, vector<string> attributeScheme, vector<string> attributeName, string primaryKey, vector<int>unique);
	//删除table，删除成功返回true，否则返回false。
	bool dropTable(string tableName);
	//返回table中所有属性的类型（储存在一个vector向量中，向量中每个值按顺序分别是每个属性的类型：int，float……)
	vector<string> getAttributeScheme(string tableName);
	//返回table中所有属性的名字（储存在一个vector向量中，向量中每个值按顺序分别是每个属性的名字：bno，cno……)
	vector<string> getAttributeName(string tableName);
	//返回table中有多少个属性
	int getAttributeNum(string tableName);
	//给定表名和一条record中的所有属性值，检验这个record里的所有属性值是否满足其对应的属性类型。满足则返回true。注：float类型的值为：1或 .1 或 1. 时，均认为其正确。
	bool judgeAttribute(string tableName, vector <string> attributeValue);
	//给定表名，把表中所有unique以及primary key 的属性的位置放入一个整型vector，返回这个vector。如：table的5个attribute中，第0，3个属性是unique，就返回向量[0,3]。
	vector<int> getUnique(string tableName);
	//给定表名，返回主键的位置
	int getPrimaryKey(string tableName);
	//给定一个表名或属性名。返回该属性名在该表中是第几个，若该属性名不在表中则返回-1。注：表中的第一个属性名返回值为0……以此类推。
	int getAttributePosition(string tableName, string attributeName);
	//创建索引
	bool createIndex(string tableName, string attributeName, string indexName);
	//删除索引
	bool dropIndex(string indexName);
	//给定表名和属性名，寻找返回该属性对应的索引的名字。若该属性无索引，则返回空字符串
	string getIndexName(string tableName, string attributeName);
	//判断该索引是否存在，如果该索引名已经存在，则返回true，否则返回false
	bool checkIndex(string indexName);
};
#endif
