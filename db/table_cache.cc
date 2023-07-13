// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "db/version_set.h"
#include <fcntl.h>
#include <iostream>
#include <table/filter_block.h>

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "table/block.h"
#include "util/coding.h"

#include "mod/stats.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  //  adgMod::Stats* instance = adgMod::Stats::GetInstance();
  //
  //
  //  Status s;
  //  char buf[sizeof(file_number)];
  //  EncodeFixed64(buf, file_number);
  //  Slice key(buf, sizeof(buf));
  //  Cache::Handle* cache_handle = cache_->Lookup(key);
  //  TableAndFile* tf = cache_handle != nullptr ?
  //  reinterpret_cast<TableAndFile*>(cache_->Value(cache_handle)) : new
  //  TableAndFile;
  //
  //  if (cache_handle == nullptr) {
  //      std::string fname = TableFileName(dbname_, file_number);
  //
  //      s = env_->NewRandomAccessFile(fname, &tf->file);
  //      if (!s.ok()) {
  //          std::string old_fname = SSTTableFileName(dbname_, file_number);
  //          if (env_->NewRandomAccessFile(old_fname, &tf->file).ok()) {
  //              s = Status::OK();
  //          }
  //      }
  //  }
  //
  //  if (cache_handle == nullptr || tf->table == nullptr) {
  //      if (s.ok()) {
  //          s = Table::Open(options_, tf->file, file_size, &tf->table);
  //      }
  //  }
  //
  //  if (cache_handle == nullptr) {
  //      if (s.ok()) {
  //          cache_handle = cache_->Insert(key, tf, 1, DeleteEntry);
  //      } else {
  //          assert(tf->table == nullptr);
  //          delete tf->file;
  //      }
  //  }
  //
  //  *handle = cache_handle;
  //  return s;

  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);

  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }

    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&),
                       int level, FileMetaData* meta, uint64_t lower,
                       uint64_t upper, bool learned, Version* version,
                       adgMod::LearnedIndexData** model, bool* file_learned) {
  Cache::Handle* handle = nullptr;
  adgMod::Stats* instance = adgMod::Stats::GetInstance();

  if ((adgMod::MOD == 6 || adgMod::MOD == 7 || adgMod::MOD == 9)) {
    // check if file model is ready
    *model = adgMod::file_data->GetModel(meta->number);
    assert(file_learned != nullptr);
    *file_learned = (*model)->Learned();

    // if level model is used or file model is available, go Bourbon path
    if (learned || *file_learned) {
      LevelRead(options, file_number, file_size, k, arg, handle_result, level,
                meta, lower, upper, learned, version);
      return Status::OK();
    }
  }

  // else, go baseline path

#ifdef INTERNAL_TIMER
  instance->StartTimer(1);
#endif
  Status s = FindTable(file_number, file_size, &handle);
#ifdef INTERNAL_TIMER
  instance->PauseTimer(1);
#endif
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result, level, meta, lower,
                       upper, learned, version);
    cache_->Release(handle);
  }
#ifdef RECORD_LEVEL_INFO
  adgMod::levelled_counters[2].Increment(level);
#endif
  return s;
}

int64_t TableCache::GetForCompaction(
    const ReadOptions& options, const Comparator* comparator, const Slice& target_key, uint64_t file_number,
    uint64_t file_size, const Slice& k, void* arg,
    void (*handle_result)(void*, const Slice&, const Slice&), int level,
    FileMetaData* meta, uint64_t lower, uint64_t upper, bool learned,
    Version* version, adgMod::LearnedIndexData** model, bool* file_learned) {
  Cache::Handle* handle = nullptr;
  adgMod::Stats* instance = adgMod::Stats::GetInstance();

  if ((adgMod::MOD == 6 || adgMod::MOD == 7 || adgMod::MOD == 9)) {
    // std::cout<<"file number"<<file_number<<std::endl;
    //  check if file model is ready
    *model = adgMod::file_data->GetModel(meta->number);
    assert(file_learned != nullptr);
    *file_learned = (*model)->Learned();

    // if level model is used or file model is available, go Bourbon path
    if (learned || *file_learned) {
      //std::cout<<"model present"<<std::endl;
      uint64_t limit = LevelReadForCompaction(options,
          comparator, target_key, file_number, file_size, k, arg, handle_result,
          level, meta, lower, upper, learned, version);
      return limit;
      // return Status::OK();
    } else {
      //std::cout<<"no model"<<std::endl;
      return -1;
    }
  }

  // else, go baseline path

  // #ifdef INTERNAL_TIMER
  //   instance->StartTimer(1);
  // #endif
  //   Status s = FindTable(file_number, file_size, &handle);
  // #ifdef INTERNAL_TIMER
  //   instance->PauseTimer(1);
  // #endif
  //   if (s.ok()) {
  //       Table* t =
  //       reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table; s =
  //       t->InternalGet(options, k, arg, handle_result, level, meta, lower,
  //       upper, learned, version); cache_->Release(handle);
  //   }
  // #ifdef RECORD_LEVEL_INFO
  //   adgMod::levelled_counters[2].Increment(level);
  // #endif
  //   return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

bool TableCache::FillData(const ReadOptions& options, FileMetaData* meta,
                          adgMod::LearnedIndexData* data) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(meta->number, meta->file_size, &handle);

  if (s.ok()) {
    Table* table =
        reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    table->FillData(options, data);
    cache_->Release(handle);
    return true;
  } else
    return false;
}

static void DeleteFilterAndFile(const Slice& key, void* value) {
  auto* filter_and_file = reinterpret_cast<FilterAndFile*>(value);
  delete filter_and_file;
}

Cache::Handle* TableCache::FindFile(const ReadOptions& options,
                                    uint64_t file_number, uint64_t file_size) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice cache_key(buf, sizeof(buf));
  Cache::Handle* cache_handle = cache_->Lookup(cache_key);

  if (cache_handle == nullptr) {
    RandomAccessFile* file = nullptr;

    // Create new file
    std::string filename = TableFileName(dbname_, file_number);
    env_->NewRandomAccessFileLearned(filename, &file);

    //        if (adgMod::use_filter) {
    //
    //            // Load footer
    //            char footer_scratch[Footer::kEncodedLength];
    //            Slice footer_slice;
    //            Status s = file->Read(file_size - Footer::kEncodedLength,
    //            Footer::kEncodedLength, &footer_slice, footer_scratch);
    //            assert(s.ok());
    //            Footer footer;
    //            s = footer.DecodeFrom(&footer_slice);
    //            assert(s.ok());
    //
    //            if (options_.filter_policy != nullptr) {
    //                // Load meta index block
    //
    //                BlockContents meta_contents;
    //                s = ReadBlock(file, options, footer.metaindex_handle(),
    //                &meta_contents); assert(s.ok()); Block* meta_block = new
    //                Block(meta_contents); Iterator* meta_iter =
    //                meta_block->NewIterator(BytewiseComparator()); string
    //                filter_name = "filter." + (string)
    //                options_.filter_policy->Name();
    //                meta_iter->Seek(filter_name);
    //                assert(meta_iter->Valid() && meta_iter->key() ==
    //                filter_name);
    //
    //                // Load filter meta block
    //                Slice filter_handle_slice = meta_iter->value();
    //                BlockHandle filter_handle;
    //                s = filter_handle.DecodeFrom(&filter_handle_slice);
    //                assert(s.ok());
    //                BlockContents filter_contents;
    //                s = ReadBlock(file, options, filter_handle,
    //                &filter_contents); filter = new
    //                FilterBlockReader(options_.filter_policy,
    //                filter_contents.data);
    //            }
    //        }

    // Insert Cache
    TableAndFile* tf = new TableAndFile;
    tf->file = file;
    tf->table = nullptr;
    // Table::Open(options_, tf->file, file_size, &tf->table);
    cache_handle = cache_->Insert(cache_key, tf, 1, DeleteEntry);
  }

  return cache_handle;
}

uint64_t TableCache::LevelReadForCompaction(
    const ReadOptions& options, const Comparator* comparator, const Slice& target_key, uint64_t file_number,
    uint64_t file_size, const Slice& k, void* arg,
    void (*handle_result)(void*, const Slice&, const Slice&), int level,
    FileMetaData* meta, uint64_t lower, uint64_t upper, bool learned,
    Version* version) {
//std::cout<<"target key: "<<target_key.ToString()<<std::endl;

  

    ParsedInternalKey parsed_key;
    ParseInternalKey(k, &parsed_key);
  adgMod::Stats* instance = adgMod::Stats::GetInstance();
  // std::cout<<"target key: "<<target_key.ToString()<<"done"<<std::endl;

  // Get the file
#ifdef INTERNAL_TIMER
  instance->StartTimer(1);
#endif
  // Cache::Handle* cache_handle = FindFile(options, file_number, file_size);
  Cache::Handle* cache_handle = nullptr;
  Status s = FindTable(file_number, file_size, &cache_handle);
  TableAndFile* tf =
      reinterpret_cast<TableAndFile*>(cache_->Value(cache_handle));
  RandomAccessFile* file = tf->file;
  FilterBlockReader* filter = tf->table->rep_->filter;
#ifdef INTERNAL_TIMER
  instance->PauseTimer(1);
#endif

  if (!learned) {
    // if level model is not used, consult file model for predicted position
#ifdef INTERNAL_TIMER
    instance->StartTimer(2);
#endif
    
    adgMod::LearnedIndexData* model = adgMod::file_data->GetModel(meta->number);
    auto bounds = model->GetPosition(parsed_key.user_key);
    lower = bounds.first;
    upper = bounds.second;
#ifdef INTERNAL_TIMER
    instance->PauseTimer(2);
#endif
    if (lower > model->MaxPosition()) {
      return model->MaxPosition();
    }
#ifdef RECORD_LEVEL_INFO
    adgMod::levelled_counters[1].Increment(level);
  } else {
    adgMod::levelled_counters[0].Increment(level);
#endif
  }

  // Get the position we want to read
  // Get the data block index
  size_t index_lower = lower / adgMod::block_num_entries;
  size_t index_upper = upper / adgMod::block_num_entries;

  // if the given interval overlaps two data block, consult the index block to
  // get the largest key in the first data block and compare it with the target
  // key to decide which data block the key is in
  uint64_t i = index_lower;
  if (index_lower != index_upper) {
    Block* index_block = tf->table->rep_->index_block;
    uint32_t mid_index_entry =
        DecodeFixed32(index_block->data_ + index_block->restart_offset_ +
                      index_lower * sizeof(uint32_t));
    uint32_t shared, non_shared, value_length;
    const char* key_ptr =
        DecodeEntry(index_block->data_ + mid_index_entry,
                    index_block->data_ + index_block->restart_offset_, &shared,
                    &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Index Entry Corruption");
    Slice mid_key(key_ptr, non_shared+8);
    int comp = comparator->Compare(mid_key, k);
    i = comp < 0 ? index_upper : index_lower;
  }

  // Check Filter Block
  uint64_t block_offset = i * adgMod::block_size;
#ifdef INTERNAL_TIMER
  instance->StartTimer(15);
#endif
//     if (filter != nullptr && !filter->KeyMayMatch(block_offset, k)) {
// #ifdef INTERNAL_TIMER
//       auto time = instance->PauseTimer(15, true);
//       adgMod::levelled_counters[9].Increment(level, time.second -
//       time.first);
// #endif
//       cache_->Release(cache_handle);
//       return;
//     }
#ifdef INTERNAL_TIMER
  auto time = instance->PauseTimer(15, true);
  adgMod::levelled_counters[9].Increment(level, time.second - time.first);
  instance->StartTimer(5);
#endif

  // Get the interval within the data block that the target key may lie in
  size_t pos_block_lower =
      i == index_lower ? lower % adgMod::block_num_entries : 0;
  size_t pos_block_upper = i == index_upper ? upper % adgMod::block_num_entries
                                            : adgMod::block_num_entries - 1;
  // Read corresponding entries
  size_t read_size =
      (pos_block_upper - pos_block_lower + 1) * adgMod::entry_size;
  static char scratch[4096];
  Slice entries;
  s = file->Read(block_offset + pos_block_lower * adgMod::entry_size, read_size,
                 &entries, scratch);
  assert(s.ok());

#ifdef INTERNAL_TIMER
  bool first_search = true;
#endif

  // Binary Search within the interval
  uint64_t left = pos_block_lower, right = pos_block_upper;
  uint32_t shared1, non_shared1, value_length1;
  // std::cout<<"pos_block_lower: "<<pos_block_lower<<"pos_block_upper:
  // "<<pos_block_upper<<std::endl;
  const char* key_ptr1 = DecodeEntry(
      entries.data() + (left - pos_block_lower) * adgMod::entry_size,
      entries.data() + read_size, &shared1, &non_shared1, &value_length1);
  Slice left_at_start;
  left_at_start = Slice(key_ptr1, non_shared1);
   //std::cout<<"size of left at start: "<<left_at_start.size()<<std::endl;
  int flag = 0;
  int c = comparator->Compare(left_at_start, k);
  //std::cout<<"size of left at start: "<<left_at_start.size()<<" size of parsed_key.user_key: "<<parsed_key.user_key.size()<<" size of target key: "<<target_key.size()<<std::endl;
  // int c = tf->table->rep_->options.comparator->Compare(left_at_start, k);
  if (c > 0) {
    // std::cout<<"left at start> target"<<std::endl;
    if (left == 0) {
      flag = 1;
    
    } else {
      while (c > 0 && left>0) {
        left--;
        c = comparator->Compare(left_at_start, k);
        key_ptr1 = DecodeEntry(
            entries.data() + (left - pos_block_lower) * adgMod::entry_size,
            entries.data() + read_size, &shared1, &non_shared1, &value_length1);
        left_at_start = Slice(key_ptr1, non_shared1);
      }
    }
  }

  while (left < right) {
    uint32_t mid = left + (right - left + 1) / 2;
    uint32_t shared, non_shared, value_length;

    const char* key_ptr = DecodeEntry(
        entries.data() + (mid - pos_block_lower) * adgMod::entry_size,
        entries.data() + read_size, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
#ifdef INTERNAL_TIMER
    if (first_search) {
      first_search = false;
      instance->PauseTimer(5);
      instance->StartTimer(3);
    }
#endif

    Slice mid_key(key_ptr, non_shared);

    int comp = comparator->Compare(mid_key, k);
    // int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
    if (comp < 0) {
      // std::cout<<"mid: "<<mid_key.ToString()<<std::endl;
      // std::cout<<"target: "<<k.ToString()<<std::endl;
      left = mid;
    } else {
      right = mid - 1;
    }
  }
  uint32_t shared2, non_shared2, value_length2;

  const char* key_ptr2 = DecodeEntry(
      entries.data() + (left - pos_block_lower) * adgMod::entry_size,
      entries.data() + read_size, &shared2, &non_shared2, &value_length2);
  Slice key;
  key = Slice(key_ptr2, non_shared2);
  // std::cout<<"key at left after binary search: "<<key.ToString()<<std::endl;
  int compare = comparator->Compare(key, k);
  // if(compare >0) {
  //   std::cout<<"key at second error correction: "<<key.ToString()<<"target: "<<k.ToString()<<std::endl;
  // }
  // //int compare = tf->table->rep_->options.comparator->Compare(key, k);
  // if(comparator->Compare(key, parsed_key.user_key) > 0) {
  //     left-=1;
  //     key_ptr = DecodeEntry(entries.data() + (left - pos_block_lower) *
  //     adgMod::entry_size,
  //         entries.data() + read_size, &shared, &non_shared, &value_length);
  //     // key = Slice(key_ptr, non_shared);
  //     // std::cout<<"error correction needed"<<std::endl;
  //     // std::cout<<"left key: "<<key.ToString()<<std::endl;
  //     // std::cout<<"target key:
  //     "<<parsed_key.user_key.ToString()<<std::endl;
  // }
  // std::cout<<" after error correction: key at left:
  // "<<key.ToString()<<std::endl; uint32_t shared2, non_shared2, value_length2;
  // const char* key_ptr2 = DecodeEntry(entries.data() + (left -
  // pos_block_lower) * adgMod::entry_size,
  //           entries.data() + read_size, &shared2, &non_shared2,
  //           &value_length2);
  // Slice key2(key_ptr2, non_shared2);
  // if(tf->table->rep_->options.comparator->Compare(key2, k) > 0) {
  //   // key_ptr2 = DecodeEntry(entries.data() + (left - pos_block_lower) *
  //   adgMod::entry_size,
  //   //         entries.data() + read_size, &shared2, &non_shared2,
  //   &value_length2); std::cout<<"error correction"<<std::endl;
  //       left=left-1;
  // }

  uint64_t b_offset;
  if (block_offset > 0) {
    // limit = (block_offset/adgMod::entry_size )+ left;
    b_offset = (block_offset / 4133) * 125;
    // b_offset = (block_offset -8)/adgMod::entry_size ;
  } else {
    b_offset = block_offset;
  }
  // std::cout<<"block offset: "<<block_offset<<std::endl;
  // std::cout<<"b offset: "<<b_offset<<std::endl;
  // std::cout<<"left: "<<left<<std::endl;

  uint64_t limit = b_offset + left;
  // std::cout<<"block_offset: "<<block_offset<<std::endl;
  // std::cout<<"left: "<<left<<std::endl;
  // std::cout<<"entry_size: "<<adgMod::entry_size<<std::endl;
  // std::cout<<"limit: "<<limit<<std::endl;

  // decode the target entry to get the key and value (actually value_addr)
  //  uint32_t shared, non_shared, value_length;
  //  const char* key_ptr = DecodeEntry(entries.data() + (left -
  //  pos_block_lower) * adgMod::entry_size,
  //          entries.data() + read_size, &shared, &non_shared, &value_length);
  //     assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
  // #ifdef INTERNAL_TIMER
  //     if (!first_search) {
  //       instance->PauseTimer(3);
  //     } else {
  //       instance->PauseTimer(5);
  //     }
  // #endif
  // Slice key(key_ptr, non_shared), value(key_ptr + non_shared, value_length);
  // handle_result(arg, key, value);

  if (block_offset > 0) {
    limit = limit - (block_offset / 4133);
  }
  if (flag) {
    std::cout<<"limit"<<std::endl;
    limit = limit - 1;
  }
  
  return (limit);
  //     //cache handle;
  //     cache_->Release(cache_handle);
}

void TableCache::LevelRead(const ReadOptions& options, uint64_t file_number,
                           uint64_t file_size, const Slice& k, void* arg,
                           void (*handle_result)(void*, const Slice&,
                                                 const Slice&),
                           int level, FileMetaData* meta, uint64_t lower,
                           uint64_t upper, bool learned, Version* version) {
  adgMod::Stats* instance = adgMod::Stats::GetInstance();

  // Get the file
#ifdef INTERNAL_TIMER
  instance->StartTimer(1);
#endif
  // Cache::Handle* cache_handle = FindFile(options, file_number, file_size);
  Cache::Handle* cache_handle = nullptr;
  Status s = FindTable(file_number, file_size, &cache_handle);
  TableAndFile* tf =
      reinterpret_cast<TableAndFile*>(cache_->Value(cache_handle));
  RandomAccessFile* file = tf->file;
  FilterBlockReader* filter = tf->table->rep_->filter;
#ifdef INTERNAL_TIMER
  instance->PauseTimer(1);
#endif
  ParsedInternalKey parsed_key;
  ParseInternalKey(k, &parsed_key);

  if (!learned) {
    // if level model is not used, consult file model for predicted position
#ifdef INTERNAL_TIMER
    instance->StartTimer(2);
#endif

    adgMod::LearnedIndexData* model = adgMod::file_data->GetModel(meta->number);
    auto bounds = model->GetPosition(parsed_key.user_key);
    lower = bounds.first;
    upper = bounds.second;
#ifdef INTERNAL_TIMER
    instance->PauseTimer(2);
#endif
    if (lower > model->MaxPosition()) return;
#ifdef RECORD_LEVEL_INFO
    adgMod::levelled_counters[1].Increment(level);
  } else {
    adgMod::levelled_counters[0].Increment(level);
#endif
  }

  // Get the position we want to read
  // Get the data block index
  size_t index_lower = lower / adgMod::block_num_entries;
  size_t index_upper = upper / adgMod::block_num_entries;

  // if the given interval overlaps two data block, consult the index block to
  // get the largest key in the first data block and compare it with the target
  // key to decide which data block the key is in
  uint64_t i = index_lower;
  if (index_lower != index_upper) {
    Block* index_block = tf->table->rep_->index_block;
    uint32_t mid_index_entry =
        DecodeFixed32(index_block->data_ + index_block->restart_offset_ +
                      index_lower * sizeof(uint32_t));
    uint32_t shared, non_shared, value_length;
    const char* key_ptr =
        DecodeEntry(index_block->data_ + mid_index_entry,
                    index_block->data_ + index_block->restart_offset_, &shared,
                    &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Index Entry Corruption");
    Slice mid_key(key_ptr, non_shared);
    int comp = tf->table->rep_->options.comparator->Compare(
        mid_key, parsed_key.user_key);
    i = comp < 0 ? index_upper : index_lower;
  }

  // Check Filter Block
  uint64_t block_offset = i * adgMod::block_size;
#ifdef INTERNAL_TIMER
  instance->StartTimer(15);
#endif
  if (filter != nullptr &&
      !filter->KeyMayMatch(block_offset, parsed_key.user_key)) {
#ifdef INTERNAL_TIMER
    auto time = instance->PauseTimer(15, true);
    adgMod::levelled_counters[9].Increment(level, time.second - time.first);
#endif
    cache_->Release(cache_handle);
    return;
  }
#ifdef INTERNAL_TIMER
  auto time = instance->PauseTimer(15, true);
  adgMod::levelled_counters[9].Increment(level, time.second - time.first);
  instance->StartTimer(5);
#endif

  // Get the interval within the data block that the target key may lie in
  size_t pos_block_lower =
      i == index_lower ? lower % adgMod::block_num_entries : 0;
  size_t pos_block_upper = i == index_upper ? upper % adgMod::block_num_entries
                                            : adgMod::block_num_entries - 1;

  // Read corresponding entries
  size_t read_size =
      (pos_block_upper - pos_block_lower + 1) * adgMod::entry_size;
  static char scratch[4096];
  Slice entries;
  s = file->Read(block_offset + pos_block_lower * adgMod::entry_size, read_size,
                 &entries, scratch);
  assert(s.ok());

#ifdef INTERNAL_TIMER
  bool first_search = true;
#endif

  // Binary Search within the interval
  uint64_t left = pos_block_lower, right = pos_block_upper;
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = DecodeEntry(
        entries.data() + (mid - pos_block_lower) * adgMod::entry_size,
        entries.data() + read_size, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");

#ifdef INTERNAL_TIMER
    if (first_search) {
      first_search = false;
      instance->PauseTimer(5);
      instance->StartTimer(3);
    }
#endif

    Slice mid_key(key_ptr, non_shared);
    int comp = tf->table->rep_->options.comparator->Compare(
        mid_key, parsed_key.user_key);
    if (comp < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  //  (block_offset + mid)/(block_num_entries * key_size)

  // decode the target entry to get the key and value (actually value_addr)
  uint32_t shared, non_shared, value_length;
  const char* key_ptr = DecodeEntry(
      entries.data() + (left - pos_block_lower) * adgMod::entry_size,
      entries.data() + read_size, &shared, &non_shared, &value_length);
  assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");
#ifdef INTERNAL_TIMER
  if (!first_search) {
    instance->PauseTimer(3);
  } else {
    instance->PauseTimer(5);
  }
#endif
  Slice key(key_ptr, non_shared), value(key_ptr + non_shared, value_length);
  handle_result(arg, key, value);

  // cache handle;
  cache_->Release(cache_handle);
}

}  // namespace leveldb
