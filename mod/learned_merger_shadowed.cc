// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include <fstream>
#include <iostream>

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"

#include "table/merger.h"

#include "mod/learned_merger.h"

namespace leveldb {

namespace {
// TODO: Verify this is a good name.
class LearnedMergingWithShadowIterator : public Iterator {
 public:
  LearnedMergingWithShadowIterator(const Comparator* comparator,
                                   Iterator** children,
                                   Iterator** shadow_children,
                                   std::vector<std::vector<FileMetaData*>> allFiles,
                                   int n, std::vector<int> levels, ReadOptions options)
      :comparator_(comparator) ,
      mergingIterator_(NewMergingIterator(comparator, shadow_children, n)),
        learnedMergingIterator_(
            NewLearnedMergingIterator(comparator, children, allFiles, n, levels, options)) {
    learned_merge_next_duration = 0;
    standard_merge_next_duration = 0;
  }

  ~LearnedMergingWithShadowIterator() override {
    delete mergingIterator_;
    delete learnedMergingIterator_;
  }

  bool Valid() const override {
    assert(learnedMergingIterator_->Valid() == mergingIterator_->Valid());
    return mergingIterator_->Valid();
  }

  void SeekToFirst() override {
    mergingIterator_->SeekToFirst();
    learnedMergingIterator_->SeekToFirst();
  }

  void SeekToLast() override {
    assert(false);  // Not supported
  }

  void Seek(const Slice& target) override {
    assert(false);  // Not supported
  }

  void Next() override {
    auto merge_start = std::chrono::high_resolution_clock::now();
    mergingIterator_->Next();
    auto merge_end = std::chrono::high_resolution_clock::now();
    standard_merge_next_duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(merge_end -
                                                             merge_start)
            .count();

    auto learned_merge_start = std::chrono::high_resolution_clock::now();
    learnedMergingIterator_->Next();
    auto learned_merge_end = std::chrono::high_resolution_clock::now();
    learned_merge_next_duration +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            learned_merge_end - learned_merge_start)
            .count();
  }

  void Prev() override {
    assert(false);  // Not supported
  }

  MergerStats get_merger_stats() override {
    MergerStats lm = learnedMergingIterator_->get_merger_stats();
    MergerStats m = mergingIterator_->get_merger_stats();
    std::ofstream stats;
    stats.open("stats.csv", std::ofstream::app);
   // std::cout<< lm.level << "\n"<<std::endl;
    stats << lm.level << ", ";  
    stats << m.comp_count << ",";
    stats << lm.comp_count << ",";
    stats << lm.cdf_abs_error << ",";
    stats << lm.num_iterators << ",";
    stats << lm.num_items << ",";
    // stats <<" '";
    // for(int i=0; i < lm.num_iterators; i++) {
    //   stats << lm.num_items_per_list[i];
    //   if(i<lm.num_iterators - 1){
    //     stats << ", ";
    //   }
    // }
    // stats <<"' ";
    stats << "\n";
    
   // stats << standard_merge_next_duration << ",";
   // stats << learned_merge_next_duration << "\n";
   stats.flush();
    stats.close();
//std::cout<<"after close"<<std::endl;
    return m;
  }

  Slice key() const override {
    if(comparator_->Compare(mergingIterator_->key(), learnedMergingIterator_->key()) !=
           0) {
            std::cout<<"assertion"<<std::endl;
            int cmp = comparator_->Compare(mergingIterator_->key(), learnedMergingIterator_->key()) ;
           // int cmp = mergingIterator_->key().compare(learnedMergingIterator_->key());
            std::cout<<"cmp: "<< cmp<<std::endl;
            std::string s1 = mergingIterator_->key().ToString();
            std::string s2 = learnedMergingIterator_->key().ToString();
            std::cout<< s1.substr(0,10) << " " << s2.substr(0,10) << std::endl;
            int c1 = (int)s1[10]; int c2 = (int)s1[11];
            std::cout<<c1<<" "<<c2<<std::endl;
            c1 = (int)s2[10]; c2 = (int)s2[11];
            std::cout<<c1<<" "<<c2<<std::endl;
            
            std::cout<<"assertion failure:"<< mergingIterator_->key().ToString()<<" "<<learnedMergingIterator_->key().ToString()<<std::endl;
           }
    else {
      std::cout<<"keys match"<<std::endl;
    }
    assert(comparator_->Compare(mergingIterator_->key(), learnedMergingIterator_->key()) ==
           0);
    //std::cout<<"key: "<<mergingIterator_->key().ToString()<<std::endl;
    return mergingIterator_->key();
  }

  Slice value() const override { return mergingIterator_->value(); }

  Status status() const override { return mergingIterator_->status(); }

 private:
  const Comparator* comparator_;
  Iterator* mergingIterator_;
  Iterator* learnedMergingIterator_;
  uint64_t standard_merge_next_duration;
  uint64_t learned_merge_next_duration;
};
}  // namespace

Iterator* NewShadowedLearnedMergingIterator(const Comparator* comparator,
                                            Iterator** children,
                                            Iterator** shadow_children,
                                            std::vector<std::vector<FileMetaData*>> allFiles,
                                            int n, std::vector<int> levels,
                                            ReadOptions options) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new LearnedMergingWithShadowIterator(comparator, children,
                                                shadow_children, allFiles, n, levels, options);
  }
}

}  // namespace leveldb
