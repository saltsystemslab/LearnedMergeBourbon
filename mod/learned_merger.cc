// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "mod/learned_merger.h"

#include "db/db_impl.h"
#include "db/version_set.h"
#include <cmath>
#include <iostream>

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include "table/iterator_wrapper.h"

#include "mod/learned_index.h"

namespace leveldb {

namespace {
class LearnedMergingIterator : public Iterator {
 public:
  LearnedMergingIterator(const Comparator* comparator, Iterator** children,
                         std::vector<std::vector<FileMetaData*>> allFiles,
                         int n, std::vector<int> levels, ReadOptions options)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        smallest_(nullptr),
        second_smallest_(nullptr),
        smallest_iterator_index_(0),
        second_smallest_iterator_index_(0),
        n_(n),
        levels_(levels),
        allFiles_(allFiles),
        options_(options),
        next_(0),
        switch_cout_(0),
        keys_consumed_(std::vector<uint64_t>()) {
    stats_.level = levels[0];
    stats_.NoModelCount = 0;
    for (int i = 0; i < n; i++) {
      keys_consumed_.push_back(0);
      stats_.num_items_per_list.push_back(0);
      children_[i].Set(children[i]);
    }

    stats_.num_items = 0;
    stats_.cdf_abs_error = 0;
    stats_.max_abs_error = 0;
    stats_.comp_count = 0;
    stats_.num_iterators = n_;
  }

  ~LearnedMergingIterator() override {
    // TODO: Revisit this destructor. Could be a memory leak...
  }

  bool Valid() const override { return (smallest_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    smallest_ = nullptr;
    FindSmallest();
  }

  void SeekToLast() override {
    assert(false);  // Not supported
  }

  void Seek(const Slice& target) override {
    assert(false);  // Not supported
  }

  void Next() override {
    assert(Valid());
    stats_.num_items++;
    smallest_->Next();
    keys_consumed_[smallest_iterator_index_]++;
    if (HasHitLimit()) {
      FindSmallest();
    }
  }

  void Prev() override {
    assert(false);  // Not supported
  }

  Slice key() const override {
    assert(Valid());
    return smallest_->key();
  }

  Slice value() const override {
    assert(Valid());
    return smallest_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

  MergerStats get_merger_stats() override { return stats_; }

 private:
  void FindSmallest();
  bool HasHitLimit();

  const Comparator* comparator_;
  IteratorWrapper* children_;
  IteratorWrapper* smallest_;
  IteratorWrapper* second_smallest_;
  int smallest_iterator_index_;
  int second_smallest_iterator_index_;
  int n_;
  std::vector<uint64_t> keys_consumed_;
  int next_;
  int switch_cout_;
  uint64_t current_key_limit_index_;
  MergerStats stats_;
  std::vector<int> levels_;
  std::vector<std::vector<FileMetaData*>> allFiles_;
  ReadOptions options_;
};

bool LearnedMergingIterator::HasHitLimit() {
  if (keys_consumed_[smallest_iterator_index_] ==
      current_key_limit_index_ + 1) {
    return true;
  }
  assert(keys_consumed_[smallest_iterator_index_] <=
         current_key_limit_index_ + 1);
  return false;
}
void LearnedMergingIterator::FindSmallest() {
  // int flag = 0;
  if (smallest_ != nullptr && second_smallest_ != nullptr) {
    //flag = 1;
    if (smallest_->Valid() && second_smallest_->Valid()) {
        stats_.comp_count+=1;
    }
    if(smallest_->Valid() && second_smallest_->Valid() &&
    comparator_->Compare(smallest_->key(), second_smallest_->key()) < 0){
      current_key_limit_index_ = keys_consumed_[smallest_iterator_index_];
      return;
    }
    smallest_ = second_smallest_;
    smallest_iterator_index_ = second_smallest_iterator_index_;
    second_smallest_ = nullptr;
    for (int i = 0; i < n_; i++) {
      if (smallest_ == &children_[i]) {
        continue;
      }
      if (!children_[i].Valid()) {
        continue;
      }
      if (second_smallest_ != nullptr) stats_.comp_count += 1;
      if (second_smallest_ == nullptr ||
          comparator_->Compare(children_[i].key(), second_smallest_->key()) <
              0) {
        second_smallest_ = &children_[i];
        second_smallest_iterator_index_ = i;
      }
    }
  }

  else {
  IteratorWrapper* smallest = nullptr;
  IteratorWrapper* second_smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    if (!children_[i].Valid()) {
      continue;
    }

    if (smallest == nullptr) {
      smallest = &children_[i];
      smallest_iterator_index_ = i;
      stats_.comp_count -= 2;
    } else if (comparator_->Compare(children_[i].key(), smallest->key()) < 0) {
      stats_.comp_count -= 1;
      second_smallest = smallest;
      smallest = &children_[i];
      second_smallest_iterator_index_ = smallest_iterator_index_;
      smallest_iterator_index_ = i;
    } else if (second_smallest == nullptr ||
               comparator_->Compare(children_[i].key(),
                                    second_smallest->key()) < 0) {
      if (second_smallest == nullptr) stats_.comp_count -= 1;
      second_smallest = &children_[i];
      second_smallest_iterator_index_ = i;
    }
    stats_.comp_count += 2;
  }

if(smallest_ == smallest){
      current_key_limit_index_ = keys_consumed_[smallest_iterator_index_];
      return;
    }
  smallest_ = smallest;
  second_smallest_ = second_smallest;
  }
  if (smallest_ == nullptr) {
    return;
  }

  if (second_smallest_ == nullptr) {
    size_t smallest_iterator_size = 0;
    for (auto i : allFiles_[smallest_iterator_index_]) {
      smallest_iterator_size += i->num_keys;
    }
    current_key_limit_index_ = smallest_iterator_size - 1;
    return;
  }

  ParsedInternalKey parsed;
  ParseInternalKey(second_smallest_->key(), &parsed);
  SequenceNumber snapshot;
  if (options_.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options_.snapshot)->sequence_number();
  } else {
    snapshot = adgMod::db->versions_->LastSequence();
  }
  LookupKey lkey(parsed.user_key, snapshot);

  std::string value;
  Version* v = adgMod::db->versions_->current();

  int file_count = 0;
  auto file_limit = v->GetLimit(keys_consumed_[smallest_iterator_index_], stats_, options_,
      file_count, comparator_, second_smallest_->key(), lkey, &value,
      levels_[smallest_iterator_index_], allFiles_[smallest_iterator_index_]);
  //std::cout<<"file_limit: "<<file_limit<<std::endl;
  size_t file_offset = 0;
  if (levels_[smallest_iterator_index_] != 0) {
    for (int file_index = 0; file_index < file_count; file_index++) {
      auto file = allFiles_[smallest_iterator_index_][file_index];
        file_offset = file_offset + file->num_keys;
    }
  }

  // if no model present, file_limit is -1
  if (file_limit == -1) {
    current_key_limit_index_ = keys_consumed_[smallest_iterator_index_];
    return;
  }

  current_key_limit_index_ = file_offset + file_limit;
  //std::cout<<"fileoffset: "<<file_offset<<std::endl;
  current_key_limit_index_ = std::max(current_key_limit_index_, keys_consumed_[smallest_iterator_index_]);
  //std::cout<<"current_key_limit_index_: "<<current_key_limit_index_<<std::endl;
  //std::cout<<"smallest iteartor index: "<<smallest_iterator_index_<<std::endl;
  //std::cout<<"keys_consumed_[smallest_iterator_index_]: "<<keys_consumed_[smallest_iterator_index_]<<std::endl;
}

}  // namespace

Iterator* NewLearnedMergingIterator(
    const Comparator* comparator, Iterator** children,
    std::vector<std::vector<FileMetaData*>> allFiles, int n,
    std::vector<int> levels, ReadOptions options) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new LearnedMergingIterator(comparator, children, allFiles, n, levels,
                                      options);
  }
}

}  // namespace leveldb