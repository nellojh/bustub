#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
const T *_get(std::shared_ptr<bustub::TrieNode> node, std::string_view key) {
  if (node->children_.find(key[0]) != node->children_.end()) {  // 当前找到了目标字符
    // 会有两种情况
    // 1.当前字符是最后一个字符，直接返回
    //  2.当前字符不是最后一个字符，继续往下找

    auto it = node->children_.find(key[0]);
    std::shared_ptr new_kid = it->second->Clone();
    std::string_view sub_key(key.data() + 1, key.size() - 1);
    auto res = _get<T>(new_kid, sub_key);
    return res;
  } else {
    if (key.size() == 0) {
      if (node->is_value_node_) {
        const bustub::TrieNodeWithValue<T> *result = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
        if (result == nullptr) {  // 特判找到为空的情况，因为可能T不匹配
          return nullptr;
        }
        return result->value_.get();
      } else {
        return nullptr;
      }
    }

    return nullptr;
  }
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {  // 递归
  //  throw NotImplementedException("Trie::Get is not implemented.");
  if (key.empty()) {
    const bustub::TrieNodeWithValue<T> *tnwv = dynamic_cast<const bustub::TrieNodeWithValue<T> *>(root_.get());
    return tnwv == nullptr ? nullptr : tnwv->value_.get();
  }
  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<TrieNode> cur = root_->Clone();
  return _get<T>(cur, key);
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  // 遍历 Trie 来查找与给定键对应的节点，并使用 dynamic_cast 将节点指针转换为
  // const TrieNodeWithValue<T>* 类型的指针。如果转换成功，则返回该值；如果转换失败或节点不存在，则返回 nullptr。
}

template <class T>
void put_(std::shared_ptr<bustub::TrieNode> root, std::string_view key, T val) {
  // 看看子节点里是否存在key[0]
  char cur_char = key.at(0);
  if (root->children_.find(cur_char) != root->children_.end()) {  // 存在该字符的点
    if (key.size() == 1) {                                        // 存在说明需要覆盖这个子节点的值
      // 新建一个点
      std::shared_ptr<T> val_ptr = std::make_shared<T>(std::move(val));
      TrieNodeWithValue<T> new_kid = TrieNodeWithValue(root->children_.at(cur_char)->children_, std::move(val_ptr));
      //      root->children_.at(cur_char)=std::make_shared<TrieNodeWithValue<T>>(new_kid);
      root->children_.find(cur_char)->second = std::make_shared<TrieNodeWithValue<T>>(new_kid);
    } else {
      std::shared_ptr<TrieNode> next = root->children_.at(cur_char)->Clone();

      std::string_view sub_key(key.data() + 1, key.size() - 1);
      put_(next, sub_key, std::move(val));
      root->children_.find(cur_char)->second = std::shared_ptr<const TrieNode>(next);
    }
  } else {  // 没找到
    if (key.size() == 1) {
      std::shared_ptr<TrieNode> new_next = nullptr;
      std::shared_ptr<T> val_ptr = std::make_shared<T>(std::move(val));
      new_next = std::make_shared<TrieNodeWithValue<T>>(std::move(val_ptr));
      root->children_.insert({cur_char, new_next});
    } else {
      std::shared_ptr<TrieNode> new_next = nullptr;
      new_next = std::make_shared<TrieNode>();
      std::string_view sub_key(key.data() + 1, key.size() - 1);
      put_(new_next, sub_key, std::move(val));
      root->children_.insert({cur_char, new_next});
    }
  }
}
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  if (key.size() == 0) {  // 说明当前的root就是要插入数值的点
    std::shared_ptr<T> val = std::make_shared<T>(std::move(value));
    std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
    if (root_->children_.empty()) {  // 当前点为空时，
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(val));
    } else {  // 如果当前节点不为空，进行覆盖操作
      new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val));
    }
    return Trie(std::move(new_root));
  }

  // key还没有走完的时候
  // 注意要求实现   ！！！写时拷贝！！！
  std::shared_ptr<bustub::TrieNode> new_root = nullptr;
  if (root_ == nullptr) {  // 当前点为空
    new_root = std::make_unique<TrieNode>();
  } else {  // 当前点存在root
    new_root = root_->Clone();
  }
  put_<T>(new_root, key, std::move(value));
  return Trie(std::move(new_root));

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

bool _remove(std::shared_ptr<TrieNode> root, std::string_view key) {
  if (key.size() == 1) {                                             // 特判最后一次操作时
    if (root->children_.find(key.at(0)) != root->children_.end()) {  // 说明存在这个点
      // 判断这个点是否存在子节点
      auto it = root->children_.find(key.at(0));
      auto kid = it->second;
      if (kid->children_.empty()) {  // 这个子节点是叶子节点
        root->children_.erase(key.at(0));
        return true;  // 出现了节点删除的情况
      } else {
        std::shared_ptr<TrieNode> new_kid = std::make_shared<TrieNode>(kid->children_);
        it->second = new_kid;
        return false;  // 只是单纯的替换，不存在节点删除的情况。
      }
    } else {  // 不存在这个节点，说明没东西可以删除的
      return false;
    }
  } else {  // 不是最后一次操作
    std::string_view sub_key(key.data() + 1, key.size() - 1);
    if (root->children_.find(key.at(0)) != root->children_.end()) {  // 说明存在这个点
      if (root->children_.size() > 1) {  // 子节点>1哪怕删除了也不会影响这个节点
        auto it = root->children_.find(key.at(0));
        // 写时复制，必须操作Clone对象，且这里不能定义为auto
        std::shared_ptr<bustub::TrieNode> kid = it->second->Clone();
        _remove(kid, sub_key);  // 子节点是否删除
        it->second = kid;
        return false;
      } else if (root->children_.size() == 1) {  // 子节点个数为1
        auto it = root->children_.find(key.at(0));
        // 写时复制，必须操作Clone对象，且这里不能定义为auto
        std::shared_ptr<bustub::TrieNode> kid = it->second->Clone();
        auto res = _remove(kid, sub_key);  // 子节点是否删除
        it->second = kid;
        if (res == true) {  // 唯一的一个子节点出现删除情况了，那么需要判断子节点，需要判断是否需要进行删除操作
          if (kid->is_value_node_) {  // 当前子节点是一个有值节点，不能删除
            return false;
          } else {
            root->children_.erase(key.at(0));
            return true;
          }
        } else {
          return false;  // 说明子节点没有进行删除操作
        }
      } else {  // 子节点已经不存在了但是路线还没走完，说明该key一定不存在
        return false;
      }
    } else {  // 不存在这个节点，说明没东西可以删除的
      return false;
    }
  }
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  // 删除的逻辑就是找点删除，最后再进行一次节点清洗

  auto node = root_;
  if (node == nullptr) {  // 没什么好删除的
    return *this;
  }
  // 处理key为空的特殊情况
  if (key.empty()) {
    if (node->is_value_node_) {  // 根节点代表空值，说明根节点需要删除
      if (node->children_.empty()) {
        return Trie();
      } else {
        std::shared_ptr<TrieNode> kid = std::make_shared<TrieNode>(node->children_);
        return Trie(kid);
      }
    } else {
      return *this;
    }
  }

  std::shared_ptr<TrieNode> new_root = root_->Clone();  // 复制一个操作
  // 开始删除操作

  _remove(new_root, key);
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    new_root = nullptr;
  }
  return Trie(std::move(new_root));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
