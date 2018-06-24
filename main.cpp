//
//  main.cpp
//  Interpreter
//
//  Created by Leaf on 2018/6/13.
//  Copyright © 2018年 Leaf. All rights reserved.
//

#include <iostream>
#include <vector>
#include <string.h>
#include "Interpreter.h"
Buffer *global_buffer;
using namespace std;

int main() {
    vector<string> input;
    Interpreter interpreter;
    string word;
    
    cout<<" * * * Welcome to miniSQL * * *"<<endl<<" >>";
    while(1) {
        cin>>word;
        if(word[word.length()-1] == ';') {
            if(word.length() > 1)
                input.push_back(word.substr(0, word.length()-1));
            int status = interpreter.exec(input);
            input.clear();
            // program exits
            if(status == -1)
                break;
        } else
            input.push_back(word);
        char c = cin.get();
        while(c == ' ') c = cin.get();
        if(c == '\n')
            cout<<" >>";
        else
            cin.unget();
    }
    
    return 0;
}
