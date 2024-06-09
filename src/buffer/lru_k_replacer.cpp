//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <mutex>  // NOLINT

#include "common/exception.h"

namespace bustub {
//
auto LRUKReplacer::CmpTimestamp(const LRUKReplacer::k_time &f1, const LRUKReplacer::k_time &f2) -> bool {
  return f1.second < f2.second;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { max_size_ = num_frames; }

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 *
 *
 *
 * find two list
 * first_list(<k)
 * second_list(>=k)
 */

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // 如果没有可以驱逐元素
  if (curr_size_ == 0) {
    return false;
  }
  // 看访问历史列表里，有无帧可以删除
  for (auto it = new_frame_.rbegin(); it != new_frame_.rend(); it++) {
    auto frame = *it;
    // 如果可以被删除
    if (evictable_[frame]) {
      recorded_cnt_[frame] = 0;
      new_locate_.erase(frame);
      new_frame_.remove(frame);
      curr_size_--;
      hist_[frame].clear();
      *frame_id = frame;
      return true;
    }
  }
  // 看缓存队列里有无帧可以删除
  for (auto its = cache_frame_.begin(); its != cache_frame_.end(); its++) {
    auto frames = (*its).first;
    if (evictable_[frames]) {
      recorded_cnt_[frames] = 0;
      cache_frame_.erase(its);
      cache_locate_.erase(frames);
      curr_size_--;
      hist_[frames].clear();
      *frame_id = frames;
      return true;
    }
  }
  return false;
}

/*
 *
 * 5.29 9:51
 *
 * 1.check limit
 * 2.add time_stamp
 * 3.deal with current frame_id`s record
 * 4.add to the list
 *
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // 1.
  // out of bound
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  // 2
  current_timestamp_++;

  // 3
  recorded_cnt_[frame_id]++;
  auto cnt = recorded_cnt_[frame_id];
  hist_[frame_id].push_back(current_timestamp_);

  // it is the first record we push in.
  if (cnt == 1) {
    // it is already full in the list
    // evict the bad one
    if (curr_size_ == max_size_) {
      frame_id_t frame;
      Evict(&frame);
    }

    evictable_[frame_id] = true;
    curr_size_++;
    new_frame_.push_front(frame_id);
    new_locate_[frame_id] = new_frame_.begin();
  }
  // it is the Kth item that user pick
  if (cnt == k_) {
    new_frame_.erase(new_locate_[frame_id]);
    new_locate_.erase(frame_id);
    // Kth time
    auto kth_time = hist_[frame_id].front();
    // make a new pair to record the id-time
    k_time new_cache(frame_id, kth_time);
    // find the place that the new pair need to insert to
    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
  if (cnt > k_) {
    // hist`s size is always <=k
    hist_[frame_id].erase(hist_[frame_id].begin());
    cache_frame_.erase(cache_locate_[frame_id]);
    auto kth_time = hist_[frame_id].front();
    k_time new_cache(frame_id, kth_time);
    auto it = std::upper_bound(cache_frame_.begin(), cache_frame_.end(), new_cache, CmpTimestamp);
    it = cache_frame_.insert(it, new_cache);
    cache_locate_[frame_id] = it;
    return;
  }
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (recorded_cnt_[frame_id] == 0) {
    return;
  }
  if (evictable_[frame_id] == set_evictable) {
    return;
  }
  if (evictable_[frame_id]) {
    max_size_--;
    curr_size_--;
  } else {
    max_size_++;
    curr_size_++;
  }
  evictable_[frame_id] = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from c, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (!evictable_[frame_id]) {
    throw std::exception();
  }
  if (recorded_cnt_[frame_id] == 0) {
    return;
  }
  if (recorded_cnt_[frame_id] < k_) {
    new_frame_.erase(new_locate_[frame_id]);
    new_locate_.erase(frame_id);
    recorded_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  } else {
    cache_frame_.erase(cache_locate_[frame_id]);
    cache_locate_.erase(frame_id);
    recorded_cnt_[frame_id] = 0;
    hist_[frame_id].clear();
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
