#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {
/** TODO(P1): Add implementation
 *
 * @brief Move constructor for BasicPageGuard
 *
 * When you call BasicPageGuard(std::move(other_guard)), you
 * expect that the new guard will behave exactly like the other
 * one. In addition, the old page guard should not be usable. For
 * example, it should not be possible to call .Drop() on both page
 * guards and have the pin count decrease by 2.
 */

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  page_ = that.page_;

  // delete that
  that.page_ = nullptr;
  that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  page_ = nullptr;
  bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  // delete that
  that.page_ = nullptr;
  that.bpm_ = nullptr;

  this->is_dirty_ = that.is_dirty_;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT
/** TODO(P1): Add implementation
 *
 * @brief Upgrade a BasicPageGuard to a ReadPageGuard
 *
 * The protected page is not evicted from the buffer pool during the upgrade,
 * and the basic page guard should be made invalid after calling this function.
 *
 * @return an upgraded ReadPageGuard
 */

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ != nullptr) {
    page_->RLatch();
  }
  auto read_page_guard = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;

  return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
