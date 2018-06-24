//
//  Interpreter.cpp
//  Interpreter
//
//  Created by Leaf on 2018/6/13.
//  Copyright © 2018年 Leaf. All rights reserved.
//

#include "Interpreter.h"
#include "API.h"
#include <vector>
#include <string>
#include <stdexcept>
#include <iostream>

using namespace std;

// change to lower case
void Interpreter::lower(string& str) {
    int len = (int)str.length();
    for(int i=0;i<len;i++)
        str[i] = (str[i] >= 'A' && str[i] <= 'Z')? str[i] + 'a' - 'A' : str[i];
}


// split the input with separator
vector<string> Interpreter::split(string str, char separator) {
    vector<string> pieces;
    
    while(1) {
        int index = (int)str.find(separator);
        if(index == -1)
            break;
        else {
            if(index != 0)
                pieces.push_back(str.substr(0, index));
            str = str.substr(index+1, str.length()-1);
        }
    }
    if(!str.empty())
        pieces.push_back(str);
    
    return pieces;
}


// preparations: adjust the input to a processable query.
vector<string> Interpreter::adjustQuery(vector<string> input) {
    vector<string> query;
    string action = input[0];
    int wordCount = (int)input.size();
    
    // change all the words into lower case.
    for(int i=0;i<wordCount;i++)
        lower(input[i]);
    
    if(!action.compare("select")) {
        // invalid input or select without conditions.
        if(wordCount < 6)
            return input;
        // find the key word "where".
        if(!input[4].compare("where")) {
            string condition;
            int cur = 5;
            
            // copy the first 5 words.
            for(int i=0;i<cur;i++)
                query.push_back(input[i]);
            
            // regularize the comparing condition (can deal with multiple 'and's)
            while(cur < wordCount) {
                condition.clear();
                while(cur < wordCount && input[cur].compare("and"))
                    condition.append(input[cur++]);
                int index = 0, len = (int)condition.length();
                while(index < len && condition[index]!='>' && condition[index]!='<' && condition[index]!='=')
                    index++;
                query.push_back(condition.substr(0, index));
                
                // check if the operation has double characters.
                if(index+1<len && (condition[index+1] == '>' || condition[index+1] == '<' || condition[index+1] == '='))
                    query.push_back(condition.substr(index++, 2));
                else
                    query.push_back(condition.substr(index, 1));
                index++;
                if(index < len)
                    query.push_back(condition.substr(index, condition.length()-index));
                if(cur < wordCount) {
                    query.push_back("and");
                    cur++;
                }
            }
        } else
            return input;
        
        
    } else if(!action.compare("delete")) {
        // invalid input or select without conditions.
        if(wordCount < 5)
            return input;
        // find the key word "where".
        if(!input[3].compare("where")) {
            string condition;
            int cur = 4;
            
            // copy the first 5 words.
            for(int i=0;i<cur;i++)
                query.push_back(input[i]);
            
            // regularize the comparing condition (can deal with multiple 'and's)
            while(cur < wordCount) {
                condition.clear();
                while(cur < wordCount && input[cur].compare("and"))
                    condition.append(input[cur++]);
                int index = 0, len = (int)condition.length();
                while(index < len && condition[index]!='>' && condition[index]!='<' && condition[index]!='=')
                    index++;
                query.push_back(condition.substr(0, index));
                
                // check if the operation has double characters.
                if(index+1<len && (condition[index+1] == '>' || condition[index+1] == '<' || condition[index+1] == '='))
                    query.push_back(condition.substr(index++, 2));
                else
                    query.push_back(condition.substr(index, 1));
                index++;
                if(index < len)
                    query.push_back(condition.substr(index, condition.length()-index));
                if(cur < wordCount) {
                    query.push_back("and");
                    cur++;
                }
            }
        } else
            return input;
        
        
    } else if(!action.compare("insert")) {
        if(wordCount < 4)
            return input;
        if(input[3].find("values") == -1)
            return input;
        string condition;
        for(int i=3;i<wordCount;i++)
            condition.append(input[i]);
        int len = (int)condition.length();
        // at least it should have '(' and ')'
        if(len < 8)
            return input;
        condition = condition.substr(6);
        for(int i=0;i<3;i++)
            query.push_back(input[i]);
        query.push_back("values");
        query.push_back(condition);
    } else
        return input;
    
    return query;
}


// the executor
int Interpreter::exec(vector<string> input) {
    clock_t start_t = clock();
    
    // prepare for the grammar check
    vector<string> query = adjustQuery(input);
    
    int wordCount = (int)query.size();
    vector<Condition> conditionVec;
    string action = query[0], tableName;
    bool valid = false;
    
    // select operation:
    // valid is used because there are many possibilities.
    if(!action.compare("select")) {
        if(wordCount < 3 || query[2].compare("from"))
            cout<<"Syntax error: select query requires \"from\"."<<endl;
        else if(wordCount == 3)
            cout<<"Syntax error: table name is missing."<<endl;
        else {
            tableName = query[3];
            if(wordCount == 4)
                valid = true;
            else if(query[4].compare("where"))
                cout<<"Syntax error: select query requires \"where\"."<<endl;
            else if(wordCount == 8) {
                // where-subclause has 1 condition
                string op = query[6];
                    if(!api.checkOperator(op))
                    cout<<"Fail to select because "<<op<<" is NOT a valid operator."<<endl;
                else
                    valid = true;
            } else if(wordCount == 12 && !query[8].compare("and")) {
                string op1 = query[6], op2 = query[10];
                if(!api.checkOperator(op1)) {
                    cout<<"Fail to select because "<<op1<<" is NOT a valid operator."<<endl;
                } else if(!api.checkOperator(op2)) {
                    cout<<"Fail to select because "<<op2<<" is NOT a valid operator."<<endl;
                } else
                    valid = true;
            } else
                cout<<"Syntax error: where-subclause is incomplete."<<endl;
        }
        
        // check the table name and the attribute name
        if(valid) {
            if(!api.checkTable(tableName)) {
                cout<<"Fail to select because table "<<tableName<<" doesn't exist."<<endl;
                valid = false;
            } else if(wordCount > 4) {
                string attr = query[5], op = query[6];
                // the first condition
                if(api.findAttribute(tableName, attr) == -1) {
                    cout<<"Fail to select because attribute "<<attr<<" doesn't exist in table "<<tableName<<"."<<endl;
                    valid = false;
                } else
                    conditionVec.push_back(Condition(api.findAttribute(tableName, attr), op, query[7]));
                
                // the second condition
                if(valid && wordCount == 12) {
                    attr = query[9];
                    op = query[10];
                    if(api.findAttribute(tableName, attr) == -1) {
                        cout<<"Fail to select because attribute "<<attr<<" doesn't exist in table "<<tableName<<"."<<endl;
                        valid = false;
                    } else
                        conditionVec.push_back(Condition(api.findAttribute(tableName, attr), op, query[11]));
                }
            }
        }
        
        // execute select operation
        if(valid && !api.select(tableName, conditionVec))
            cout<<"Fail to select because of unknown error."<<endl;
    }
    
    
    
    
    // delete operation:
    // valid is used for the same reason as select.
    else if(!action.compare("delete")) {
        if(wordCount == 1 || query[1].compare("from"))
            cout<<"Syntax error: delete query requires \"into\"."<<endl;
        else if(wordCount == 2)
            cout<<"Syntax error: table name is missing."<<endl;
        else {
            tableName = query[2];
            if(wordCount == 3)
                valid = true;
            else if(query[3].compare("where"))
                cout<<"Syntax error: delete query requires \"where\"."<<endl;
            else if(wordCount == 7) {
                // where-subclause has 1 condition
                string op = query[5];
                if(!api.checkOperator(op))
                    cout<<"Fail to delete because "<<op<<" is NOT a valid operator."<<endl;
                else
                    valid = true;
            } else if(wordCount == 11 && !query[7].compare("and")) {
                string op1 = query[5], op2 = query[9];
                if(!api.checkOperator(op1)) {
                    cout<<"Fail to delete because "<<op1<<" is NOT a valid operator."<<endl;
                } else if(!api.checkOperator(op2)) {
                    cout<<"Fail to delete because "<<op2<<" is NOT a valid operator."<<endl;
                } else
                    valid = true;
            } else
                cout<<"Syntax error: where-subclause is incomplete."<<endl;
        }
        
        // check the table name and the attribute name
        if(valid) {
            if(!api.checkTable(tableName)) {
                cout<<"Fail to delete because table "<<tableName<<" doesn't exist."<<endl;
                valid = false;
            } else if(wordCount > 3) {
                string attr = query[4], op = query[5];
                // the first condition
                if(api.findAttribute(tableName, attr) == -1) {
                    cout<<"Fail to delete because attribute "<<attr<<" doesn't exist in table "<<tableName<<"."<<endl;
                    valid = false;
                } else
                    conditionVec.push_back(Condition(api.findAttribute(tableName, attr), op, query[6]));
                
                // the second condition
                if(valid && wordCount == 11) {
                    attr = query[8];
                    op = query[9];
                    if(api.findAttribute(tableName, attr) == -1) {
                        cout<<"Fail to delete because attribute "<<attr<<" doesn't exist in table "<<tableName<<"."<<endl;
                        valid = false;
                    } else
                        conditionVec.push_back(Condition(api.findAttribute(tableName, attr), op, query[10]));
                }
            }
        }
        
        // execute delete operation
        if(valid)
            cout<<"Okay, "<<api.delete_(tableName, conditionVec)<<" row(s) has(have) been deleted."<<endl;
    }
    
    
        
        
    // create operation:
    else if(!action.compare("create")) {
        if(wordCount == 1 || ((query[1].find("table") == -1 && query[1].find("index") == -1)))
            cout<<"Syntax error: create query requires a specification of type (\"table\" or \"index\", in exact)."<<endl;
        else if(!query[1].compare("table")) {
            if(wordCount < 3)
                cout<<"Syntax error: table name is missing."<<endl;
            else {
                string subclause;
                for(int i=2;i<wordCount;i++) {
                    subclause.append(query[i]);
                    subclause.append(" ");
                }
                int len = (int)subclause.length(), leftPar = (int)subclause.find("(");
                subclause = subclause.substr(0, len-1);
                len--;
                if(leftPar == 0)
                    cout<<"Syntax error: table name is missing."<<endl;
                else if(leftPar == -1 || subclause[len-1] != ')')
                    cout<<"Syntax error: invalid parenthese format detected."<<endl;
                else {
                    tableName = subclause.substr(0, leftPar);
                    if(!api.checkLegal(tableName))
                        cout<<"Fail to create table because of illegal characters in \""<<tableName<<"\"."<<endl;
                    else {
                        len -= tableName.length();
                        subclause = subclause.substr(leftPar+1);
                        vector<string> attributes = split(subclause, ',');
                        cout<<"processing attributes..."<<endl;
                        
                        
                        // process the attributes...
                        int attrNum = (int)attributes.size()-1;
                        vector<string> attrName;
                        vector<string> attrType;
                        vector<int> uniqIndex;
                        int primKeyIndex = -1, type = -1;
                        valid = true;

                        
                        // record the information of the attributes.
                        for(int i=0;i<attrNum;i++) {
                            int len = (int)attributes[i].length();
                            if(attributes[i][0] == ' ') {
                                attributes[i] = attributes[i].substr(1);
                                len--;
                            }
                            if(attributes[i][len-1] == ' ') {
                                attributes[i] = attributes[i].substr(0, len-1);
                                len--;
                            }
                            
                            // separate the attribute by ' '
                            // check the number of arguments.
                            vector<string> attr = split(attributes[i], ' ');
                            string charLen;
                            bool unique = false;
                            
                            if(attr.size() < 2) {
                                cout<<"Syntax error: too few words to define an attribute in \""<<attributes[i]<<"\"."<<endl;
                                valid = false;
                                break;
                            }
                            
                            // check attribute's name
                            if(!api.checkLegal(attr[0])) {
                                cout<<"Fail to create table because of illegal characters in \""<<attributes[i]<<"\"."<<endl;
                                valid = false;
                                break;
                            }
                            
                            // check attribute's type
                            if(!attr[1].compare("int"))
                                type = 0;
                            else if(!attr[1].compare("float"))
                                type = 1;
                            else if(!attr[1].find("char")){
                                if(attr[1].length() < 7 || attr[1][4] != '(' || attr[1][attr[1].size()-1] != ')') {
                                    cout<<"Syntax error: specification of attribute type for char is incomplete."<<endl;
                                    valid = false;
                                    break;
                                }
                                
                                charLen = attr[1].substr(5, attr[1].size()-6);
                                if(!api.checkInteger(charLen)) {
                                    cout<<"Syntax error: \""<<charLen<<"\" is not an positive integer within 255."<<endl;
                                    valid = false;
                                    break;
                                }
                                type = 2;
                            } else {
                                cout<<"Syntax error: specification of attribute type is missing."<<endl;
                                valid = false;
                                break;
                            }
                            
                            // check if the attribute is unique.
                            if(attr.size() == 3) {
                                if(attr[2].compare("unique")) {
                                    cout<<"Syntax error: fail to recognize key word \""<<attr[2]<<"\"; did you mean \"unique\"?"<<endl;
                                    valid = false;
                                    break;
                                }
                                else
                                    unique = true;
                            }
                            
                            // push all the information of this attribute.
                            if(valid) {
                                // name of attribute
                                attrName.push_back(attr[0]);
                                // type of attribute
                                if(type == 0)
                                    attrType.push_back("int");
                                else if(type == 1)
                                    attrType.push_back("float");
                                else {
                                    string tmp = "char";
                                    tmp.append(charLen);
                                    attrType.push_back(tmp);
                                }
                                // uniqueness of attribute
                                if(unique)
                                    uniqIndex.push_back(i);
                            }
                        }
                        
                        // check the primary key...
                        string primInfo = attributes[attrNum];
                        int plen = (int)primInfo.length();
                        if(primInfo[0] == ' ') {
                            primInfo = primInfo.substr(1);
                            plen--;
                        }
                        // the last character is ')', which should not be included.
                        primInfo = primInfo.substr(0, --plen);
                        if(primInfo.find("primary key(") != 0 || primInfo.find(")") == -1) {
                            cout<<"Syntax error: primary key information is incomplete."<<endl;
                            valid = false;
                        }
                        else {
                            string primAttr = primInfo.substr(12);
                            int ppLen = (int)primAttr.length();
                            primAttr = primAttr.substr(0, ppLen-1);
                            for(primKeyIndex=0;primKeyIndex<attrNum;primKeyIndex++)
                                if(!attrName[primKeyIndex].compare(primAttr))
                                    break;
                            if(primKeyIndex == attrNum) {
                                cout<<"Fail to create table because the name of attribute to be the primary key does not belong to this table."<<endl;
                                valid = false;
                            }
                        }
                    
                        // execute the create table operation.
                        if(valid) {
                           // primary key is also unique.
                            uniqIndex.push_back(primKeyIndex);
                            api.createTable(tableName, primKeyIndex, attrName, attrType, uniqIndex);
                            cout<<"Okay, table \""<<tableName<<"\" is created."<<endl;
                        }
                    }
                }
            }
        } else {
            if(wordCount < 3)
                cout<<"Syntax error: name of index is missing."<<endl;
            else {
                string indexName = query[2];
                if(!api.checkLegal(indexName))
                    cout<<"Fail to create index because of illegal characters in \""<<indexName<<"\""<<endl;
                else if(wordCount < 4 || query[3].compare("on"))
                    cout<<"Syntax error: create index requires \"on\"."<<endl;
                else if(wordCount < 5)
                    cout<<"Syntax error: create index requires details, including table name and attribute name."<<endl;
                else {
                    string subclause;
                    for(int i=4;i<wordCount;i++)
                        subclause.append(query[i]);
                    int len = (int)subclause.length(), leftPar = (int)subclause.find("(");
                    if(leftPar == -1 || subclause[len-1] != ')')
                        cout<<"Syntax error: invalid parenthese format detected."<<endl;
                    else {
                        tableName = subclause.substr(0, leftPar);
                        string attr = subclause.substr(leftPar+1, len-1);
                        if(!api.checkTable(tableName))
                            cout<<"Fail to create index because table "<<tableName<<" doesn't exist."<<endl;
                        else if(api.findAttribute(tableName, attr) == -1)
                            cout<<"Fail to create index because attribute "<<attr<<" doesn't exist."<<endl;
                        else if(!api.createIndex(tableName, attr, indexName))
                            cout<<"Fail to create index because of unknown error."<<endl;
                        else
                            cout<<"Successfuly created index."<<endl;
                    }
                }
            }
        }
    }
    
    
    
    
    // drop operation:
    else if(!action.compare("drop")) {
        if(wordCount == 1 || (query[1].compare("table") && query[1].compare("index")))
            cout<<"Syntax error: drop query requires a specification of type (\"table\" or \"index\", in exact)."<<endl;
        else if(wordCount == 2)
            cout<<"Syntax error: object name is missing."<<endl;
        // execute the drop operation.
        else {
            string name = query[2];
            bool status = false, dropped = false;
            if(query[1].compare("table")){
                if(!api.checkTable(name))
                    cout<<"Fail to drop table because "<<name<<" doesn't exist."<<endl;
                else {
                    dropped = true;
                    status = api.dropTable(name);
                }
            }
            else {
                if(!api.checkIndex(name))
                    cout<<"Fail to drop index because "<<name<<" doesn't exist."<<endl;
                else {
                    dropped = true;
                    status = api.dropIndex(name);
                }
            }
            if(dropped) {
                if(status)
                    cout<<"Successfully dropped "<<name<<"."<<endl;
                else
                    cout<<"Fail to drop because of unknown errors."<<endl;
            }
        }
    }
    
    
    
        
    // insert operation:
    // valid is not used in this process, because only one way leads to success.
    else if(!action.compare("insert")) {
        if(wordCount == 1 || query[1].compare("into"))
            cout<<"Syntax error: insert requires \"into\"."<<endl;
        else if(wordCount == 2)
            cout<<"Syntax error: table name is missing."<<endl;
        else {
            tableName = query[2];
            if(wordCount == 3 || query[3].compare("values"))
                cout<<"Syntax error: insert query requires \"values\"."<<endl;
            else if(wordCount == 4)
                cout<<"Syntax error: insert query requires a tuple."<<endl;
            else {
                string tuple = query[4];
                for(int i=5;i<wordCount;i++)
                    tuple.append(query[i]);
                if(tuple[0] != '(' || tuple[tuple.length()-1] != ')')
                    cout<<"Syntax error: invalid parenthese format detected."<<endl;
                else {
                    vector<string> tupleVec = split(tuple.substr(1, tuple.length()-2), ',');
                    if(!api.checkInsertFormat(tableName, tupleVec))
                        cout<<"Fail to insert because the format of inserted row does NOT match the attributes of "<<tableName<<endl;
                    else {
                        // record the indices of unique attributes in the table
                        vector<int> uniqueIndices = api.getUniqueIndices(tableName);
                        // record the index of first not unique attribute
                        int index, n = (int)uniqueIndices.size();
                        for(index=0;index<n;index++)
                            if(!api.checkUnique(tableName, uniqueIndices[index], tupleVec[index]))
                                break;
                        if(index < n)
                            cout<<"Fail to insert because "<<tupleVec[index]<<" already exists in table "<<tableName<<", and this attribute ought to be unique."<<endl;
                        // execute insert operation
                        else if(!api.insert(tableName, tupleVec))
                            cout<<"Fail to insert because of unknown errors."<<endl;
                        else
                            cout<<"Successfully inserted."<<endl;
                    }
                }
            }
        }
    }
        
    
    
        
    // quit operation:
    // return -1 to tell the main program to exit.
    else if(!action.compare("quit")) {
        if(wordCount > 1) {
            cout<<"Fail to quit because you've entered something else."<<endl;
        } else {
            cout<<"Applying changes to files..."<<endl;
            cout<<"Okay, bye."<<endl;
            return -1;
        }
    }
    // no such operation:
    else
        cout<<"Syntax error: "<<action<<" is NOT a valid operation."<<endl;
    
    
    // count the time
    cout<<"Duration: "<<1.0*(clock() - start_t)/CLOCKS_PER_SEC<<" sec."<<endl;
    
    return 0;
}
