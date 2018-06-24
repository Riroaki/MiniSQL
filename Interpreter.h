//
//  Interpreter.h
//  Interpreter
//
//  Created by Leaf on 2018/6/13.
//  Copyright © 2018年 Leaf. All rights reserved.
//

#ifndef Interpreter_h
#define Interpreter_h

#include <vector>
#include <string>
#include <iostream>
#include "API.h"
using namespace std;

class Interpreter {
private:
    void lower(string&);
    void clearSpace(string&);
    vector<string> split(string str, char separator);
    vector<string> adjustQuery(vector<string> input);
    API api;
public:
    Interpreter(){}
    int exec(vector<string> query);
};

#endif /* Interpreter_h */
