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

#include <unordered_map>
#include <algorithm>
#include <vector>
#include <cassert>


namespace cache
{

template<typename KeyType,
        typename ValueType,
        class GetValueFunc,
        ValueType NotFoundValue = ValueType(),
        unsigned int MAX_SIZE = 512>
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
            // cache miss, do the hard work
            ValueType val = func_(key);
            if (val != NotFoundValue) {
                return map_.emplace(
                        std::make_pair(
                            key, Item{ generation_, val })).first->second.value_;
            } else {
                return NotFoundValue;
            }
        } else {
            // cache hit
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

        assert(items.size() > MAX_SIZE);
        auto nthElem = items.begin() + MAX_SIZE;

        std::nth_element(items.begin(), nthElem, items.end(), genCompare);

        for (auto i = nthElem; i != items.end(); ++i) {
            map_.erase(*i);
        }
    }

    struct Item
    {
        int64_t generation_;
        ValueType value_;
    };

    typedef std::unordered_map<KeyType, Item> CacheMap;
    typedef typename CacheMap::iterator CmIterator;

    int64_t generation_ = 0;
    CacheMap map_;
    GetValueFunc func_;
};

} /* namespace cache */

#endif /* GENERICCACHE_H_ */
