/*
 * GenericCache.h - cache using a generation based LRU strategy
 *
 * This is part of the "librdf.firebird" storage module for the
 * "Redland RDF Library" (http://librdf.org/) that stores and
 * retrieves RDF data from a Firebird database.
 *
 * @created: Aug 30, 2015
 *
 * @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
 * under the terms and conditions of the Lesser GNU General
 * Public License version 2.1
 */
#ifndef GENERICCACHE_H_
#define GENERICCACHE_H_

#include <map>
#include <unordered_map>
#include <algorithm>
#include <vector>
#include <cassert>
#include <iostream> // TODO: remove
using std::cout; // TODO: remove

namespace cache
{

template<class KeyType,
        class ValueType,
        class GetValueFunc,
        unsigned int MAX_SIZE = 100>
class GenericCache
{
public:
    GenericCache(GetValueFunc func) : func_(func)
    {
    }

    ~GenericCache()
    {
    }

    ValueType getValue(KeyType key)
    {
        if (generation_++ % (2 * MAX_SIZE) == 0) {
            removeOldItems();
        }

        CmIterator i = map_.find(key);
        if (i == map_.end()) {
            ValueType val = func_(key);
            // cout << "Cache MISS: " << key << " val: " << val << " gen: " << generation_ << "\n";
            if (val) { // TODO: this is not right
                return map_.emplace(
                        std::make_pair(key, Item{ generation_, func_(key) })).first->second.value_;
            } else {
                return ValueType(); // TODO: this is not right
            }

            // Item cacheItem{generation_, func_(key)};
            // return map_.insert(std::make_pair(key, cacheItem)).first->second.value_;
        } else {
            //cout << "Cache HIT : " << key << " val: " << i->second.value_
            //     << " gen: " << generation_ << "/" << i->second.generation_ << "\n";
            i->second.generation_ = generation_;
            return i->second.value_;
        }
    }

private:

    void removeOldItems()
    {
        if (map_.size() <= MAX_SIZE) {
            return;
        }
        std::vector<typename CacheMap::iterator> items;
        items.reserve(map_.size());
        for (auto i = map_.begin(); i != map_.end(); ++i) {
            items.push_back(i);
        }

        auto genCompare = [] (CmIterator a, CmIterator b) {
            return b->second.generation_ < a->second.generation_;
        };
        // TODO: use partition instead of sort

        std::sort(items.begin(), items.end(), genCompare);

        assert(items.size() > MAX_SIZE);
        for (auto i = items.begin() + MAX_SIZE; i != items.end(); ++i) {
            // cout << "Cache erase: " << (*i)->first << " gen: " << (*i)->second.generation_ << "\n";
            map_.erase(*i);
        }
    }

    struct Item
    {
        int64_t generation_;
        ValueType value_;
    };

    //typedef std::unordered_map<KeyType, Item> CacheMap;
    typedef std::map<KeyType, Item> CacheMap;
    typedef typename CacheMap::iterator CmIterator;

    int64_t generation_ = 0;
    CacheMap map_;
    GetValueFunc func_;
};

} /* namespace cache */

#endif /* GENERICCACHE_H_ */
