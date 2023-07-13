// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_LEARNED_MERGER_H_
#define STORAGE_LEVELDB_TABLE_LEARNED_MERGER_H_

#include <map>
#include <vector>
#include "leveldb/options.h"
#include "learned_index.h"
namespace leveldb {

class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
Iterator* NewLearnedMergingIterator(const Comparator* comparator,
                                    Iterator** children,
                                    std::vector<std::vector<FileMetaData*>> allFiles,
                                     int n, std::vector<int> levels, ReadOptions options);

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// This implementation will compare it's results with the
// base MergingIterator and assert that they are equal
//
// REQUIRES: n >= 0
Iterator* NewShadowedLearnedMergingIterator(
    const Comparator* comparator, Iterator** children,
    Iterator** shadow_children,
    std::vector<std::vector<FileMetaData*>> allFiles,
    int n, std::vector<int> levels,
    ReadOptions options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_LEARNED_MERGER_H_
