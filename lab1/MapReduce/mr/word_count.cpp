#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <algorithm>
#include <cctype>
#include <glob.h>

std::map<std::string, int> countWordsInFile(const std::string& filename) {
    std::map<std::string, int> word_count;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "Cannot open file: " << filename << std::endl;
        return word_count;
    }
    
    std::string word;
    char c;
    
    while (file.get(c)) {
        if (std::isalpha(c)) {
            word += c;
        } else {
            if (!word.empty()) {
                word_count[word]++;
                word.clear();
            }
        }
    }
    
    // 处理最后一个单词
    if (!word.empty()) {
        word_count[word]++;
    }
    
    file.close();
    return word_count;
}

std::vector<std::string> glob(const std::string& pattern) {
    std::vector<std::string> files;
    glob_t glob_result;
    
    int ret = glob(pattern.c_str(), GLOB_TILDE, NULL, &glob_result);
    if (ret == 0) {
        for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
            files.push_back(std::string(glob_result.gl_pathv[i]));
        }
    }
    
    globfree(&glob_result);
    return files;
}

int main() {
    // 获取所有pg-*.txt文件
    std::vector<std::string> files = glob("pg-*.txt");
    
    if (files.empty()) {
        std::cerr << "No files matching pattern pg-*.txt found" << std::endl;
        return 1;
    }
    
    std::map<std::string, int> total_word_count;
    
    // 统计每个文件中的单词
    for (const std::string& filename : files) {
        std::cout << "Processing file: " << filename << std::endl;
        std::map<std::string, int> file_word_count = countWordsInFile(filename);
        
        // 合并到总统计中
        for (const auto& entry : file_word_count) {
            total_word_count[entry.first] += entry.second;
        }
    }
    
    // 转换为vector并排序
    std::vector<std::pair<std::string, int>> sorted_words;
    for (const auto& entry : total_word_count) {
        sorted_words.push_back(entry);
    }
    
    // 按计数降序排序
    std::sort(sorted_words.begin(), sorted_words.end(),
              [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
                  return a.second > b.second;
              });
    
    // 输出前10个最常见的单词
    std::cout << "\nTop 10 most frequent words:" << std::endl;
    for (int i = 0; i < 10 && i < sorted_words.size(); i++) {
        std::cout << sorted_words[i].first << " " << sorted_words[i].second << std::endl;
    }
    
    return 0;
}