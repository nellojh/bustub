//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <exception>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  Page *page;
  frame_id_t frame_id = -1;
  std::scoped_lock lock(latch_);
  // 分配地址
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else {
    // free space is out
    if (replacer_->Evict(&frame_id)) {
      page = pages_ + frame_id;
    } else {
      return nullptr;
    }
  }
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }

  //* You also need to reset the memory and metadata for the new page.
  *page_id = AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_.emplace(*page_id, frame_id);
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  // * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
  replacer_->RecordAccess(frame_id);
  // * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  std::scoped_lock lock(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    // 不需要从disk获取该page；
    auto frame_id = page_table_[page_id];
    auto it = pages_ + frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    it->pin_count_++;
    return it;
  }
  // 先new_page一下，从disk获取该页
  Page *page;
  frame_id_t frame_id = -1;
  // 分配地址
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else {
    // free space is out
    if (replacer_->Evict(&frame_id)) {
      page = pages_ + frame_id;
    } else {
      return nullptr;
    }
  }
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }

  page_table_.erase(page->GetPageId());
  page_table_.emplace(page_id, frame_id);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 获取后进行操作即可
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto it = pages_ + page_table_[page_id];
  // 设置脏位,如果原本是脏的或传进的is_dirty是脏的，最终就是脏的
  it->is_dirty_ = is_dirty || it->is_dirty_;
  if (it->GetPinCount() == 0) {
    return false;
  }
  it->pin_count_--;
  if (it->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto it = pages_ + page_table_[page_id];

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, it->GetData(), it->GetPageId(), std::move(promise)});
  future.get();
  it->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock(latch_);
  for (size_t current_size = 0; current_size < pool_size_; current_size++) {
    auto it = pages_ + current_size;
    if (it->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, it->GetData(), it->GetPageId(), std::move(promise)});
    future.get();
    it->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    throw std::exception();
  }
  std::scoped_lock lock(latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    auto it = pages_ + page_table_[page_id];
    if (it->GetPinCount() != 0) {
      return false;
    }
    page_table_.erase(page_id);
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    it->ResetMemory();
    it->page_id_ = INVALID_PAGE_ID;
    it->is_dirty_ = false;
    it->pin_count_ = 0;
    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
