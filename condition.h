//
//  condition.h
//  mini
//
//  Created by Leaf on 2018/6/23.
//  Copyright © 2018年 Leaf. All rights reserved.
//

#ifndef condition_h
#define condition_h

#include <string>
using namespace std;

class Condition {
public:
    int attributeIndex;
    int operationType;
    string cmpValue;
    Condition(int attrIndex, string opType, string value):attributeIndex(attrIndex), cmpValue(value) {
        if(opType.length() == 1) {
            if(opType.compare(" ") == 0)
                operationType = 0;
            else if(opType.compare(">") == 0)
                operationType = 1;
            else
                operationType = 2;
        } else {
            if(opType.compare(">="))
                operationType = 3;
            else if(opType.compare("<="))
                operationType = 4;
            else
                operationType = 5;
        }
    }
};

#endif /* condition_h */
