#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <cctype>

#ifdef _WIN32
    #define TOOLS_API __declspec(dllexport)
#else
    #define TOOLS_API __attribute__((visibility("default")))
#endif
class KeyValue{
public:
    std::string key;
    std::string value;
};
extern "C" {
    TOOLS_API std::vector<KeyValue> map_f(const std::string& filename) {
        std::vector<KeyValue> result;
        std::ifstream file(filename);
        if(!file.is_open()){
            std::cerr << "Unable to open file:" << filename << std::endl;
            return result;
        }

        char c;
        std::string currentWord;
        KeyValue kv;
        while(file.get(c)) {
            if (isalnum(c)) {
                currentWord += c;
            } else {
                if (!currentWord.empty()) {
                    kv.key = currentWord;
                    kv.value = "1";
                    result.emplace_back(kv);
                }
                currentWord.clear();
            }
        }
        

        if (!currentWord.empty()) {
            kv.key = currentWord;
            kv.value = "1";
            result.emplace_back(kv);
        }

        return result;
    }

    TOOLS_API std::string reduce_f(const std::string& key, const std::vector<std::string>& values) {
        return std::to_string(values.size());
    }
}