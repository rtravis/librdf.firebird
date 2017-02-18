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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>


namespace cache
{

template<typename KeyType,
        typename ValueType,
        class GetValueFunc,
        ValueType NotFoundValue = ValueType(),
        size_t TRIM_TO_SIZE = 512>
class GenericCache final
{
public:
    GenericCache(GetValueFunc func) : generation_(0), dict_(), func_(func)
    {
    }

    ~GenericCache()
    {
    }

    ValueType getValue(KeyType key)
    {
        if (generation_++ % (2 * TRIM_TO_SIZE) == 0) {
            removeOldItems();
        }

        CdIterator i = dict_.find(key);
        if (i == dict_.end()) {
            // cache miss, do the hard work
            ValueType val = func_(key);
            if (val != NotFoundValue) {
                return dict_.emplace(
                        std::make_pair(
                            key, Item{generation_, val})).first->second.value_;
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
        if (dict_.size() <= TRIM_TO_SIZE) {
            return;
        }

        std::vector<typename CacheDict::iterator> items;
        items.reserve(dict_.size());
        for (auto i = dict_.begin(); i != dict_.end(); ++i) {
            items.push_back(i);
        }

        auto generationCompare = [] (CdIterator a, CdIterator b) {
            return b->second.generation_ < a->second.generation_;
        };

        assert(items.size() > TRIM_TO_SIZE);
        auto nthElem = items.begin() + TRIM_TO_SIZE;

        std::nth_element(items.begin(), nthElem, items.end(), generationCompare);

        for (auto i = nthElem; i != items.end(); ++i) {
            dict_.erase(*i);
        }
    }

    struct Item
    {
        uint64_t generation_;
        ValueType value_;
    };

    typedef std::unordered_map<KeyType, Item> CacheDict;
    typedef typename CacheDict::iterator CdIterator;

    uint64_t generation_;
    CacheDict dict_;
    GetValueFunc func_;
};

} /* namespace cache */

#endif /* GENERICCACHE_H_ */
