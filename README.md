# miniSQL
2018春夏数据库大作业

## DEBUG & 优化日志
- ```功能缺失```：index的文件创建未实现 & 创建后文件名不匹配
- ```逻辑问题```：IndexManager使用函数局部buffer
- ```逻辑问题```：RecordManager与IndexManager使用了不同的buffer
  - ```笔误```：B+树函数调用错误（导致树名神秘丢失的元凶）
  - ```细节```：interpreter解析表名时考虑不周
  - ```笔误```：API匹配值类型转义符应该用\\\' 而不是\\\"
- ```功能缺失```：从文件读入index未实现
- ```逻辑问题```：B+数初始化时树名（同时也是index文件名）未赋值
  - ```细节```：BufferManager写文件缺少fclose()导致实际文件为空
- ```逻辑问题```：IndexManager建表时未将index登记在内存
- ```语法问题```：RecordManager中vector指针的多处误用（接口未明确导致）
  - ```细节```：RecordManager修改逻辑时数组越界
- ```逻辑问题```：RecordManager在内存中的记录长度与文件中的约定长度（255字）不一致导致矛盾（解决：借助CatalogManager的函数修改为可变长记录）
  - ```细节```：二进制读写导致异常的乱码（从cout改为std::printf）
  - ```细节```：preIndex文件名未与表名绑定导致无法多建表
  - ```细节```：API的delete接口返回类型为int，表示删除条数；而不是bool类型（表示操作状态）
  - ```笔误```：API的delete接口中对RecordManager中函数错误调用
  - ```语法```：动态长度的数组定义在不同的编译器环境下可能导致编译出错
  - ```优化```：优化输出表格排版
  - ```细节```：BufferManager删除表内容时，若表为空会导致memcpy出现异常的错误
- ```逻辑问题```：API的delete_接口未实现在全部相关index中删除记录
- ```优化```：API的delete接口除非在表中存在被删除的记录，否则不会调用IndexManager删除B+树中的节点
  - ```细节```：API的delete接口未考虑无条件删除的情况
- ```细节```：memcpy和memset出现的大量野指针（超过限制的长度）
- ```细节```：condition的构造函数中符号规定和处理时符号规定不一致（以及condition构造函数中笔误）
- ```逻辑问题```：IndexManager将RecordManager的block误清空（因为内存是公用的）
- ```回滚```：由于始终遭遇不知名BUG（无法复现），回滚为定长Record版本（奇迹般地可以用了）
- ```功能缺失```：实现EXECFILE指令的部分缺失（Interpreter）
- ```细节```：Blocksize超过4096字时出现问题
-```优化```：Index在创建表声明unique时即建立（事实上，在表中有数据后再新建index会存在错误，所以采取这种方式）
