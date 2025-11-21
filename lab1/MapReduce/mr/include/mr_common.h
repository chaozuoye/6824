#include <vector>
#include <map>
#include <string>
#include <cstdint>
#include <cstring>

int iHash(const std::string& key) {
    const uint32_t FNV_OFFSET_BASIS = 2166136261U;
    const uint32_t FNV_PRIME = 16777619U;
    
    uint32_t hash = FNV_OFFSET_BASIS;
    
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }
    
    return static_cast<int>(hash & 0x7fffffff);
}