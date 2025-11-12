#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <cctype>
using IntermediatePair = std::pair<std::string, std::string>;
// 词频统计的Map函数：将每行拆分为单词，输出(word, "1")
std::vector<IntermediatePair> map_f(const std::string& filename) {
    std::vector<IntermediatePair> result;
    std::map<std::string,int> key_value;
    std::ifstream file(filename);
    if(!file.is_open()){
        std::cerr << "无法打开文件：" << filename << std::endl;
        return result;
    }

    char c;
    std::string currentWord;
    while(file.get(c)) {
        if (isalnum(c)) {
            currentWord += c;
        } else {
            if (!currentWord.empty()) {
                result.emplace_back(std::make_pair(currentWord,"1"));
            }
        }
    }

    // 待实现：读取文件内容，拆分单词，生成中间键值对
    return result;
}

// 词频统计的Reduce函数：将相同单词的"1"累加，输出总次数
std::string reduce_f(const std::string& key, const std::vector<std::string>& values) {
    // 待实现：计算values的长度（即出现次数），返回字符串形式
    return std::to_string(values.size());
}