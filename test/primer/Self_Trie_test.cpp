//
// Created by ljh on 3/28/24.
//
// tests/test_example.cpp
#include <fmt/format.h>
#include <bitset>
#include <functional>
#include <numeric>
#include <optional>
#include <random>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "gtest/gtest.h"
#include "primer/trie.h"

namespace bustub {
TEST(Self_Test, TrieStructureCheck) {
  auto trie = bustub::Trie();
  // Put something
  trie = trie.Put<uint32_t>("test", 233);
  std::cout<<"111"<<"\n";
  ASSERT_EQ(*trie.Get<uint32_t>("test"), 233);
  //  // Ensure the trie is the same representation of the writeup
  //  // (Some students were using '\0' as the terminator in previous semesters)
  auto root = trie.GetRoot();
  ASSERT_EQ(root->children_.size(), 1);
  ASSERT_EQ(root->children_.at('t')->children_.size(), 1);
  ASSERT_EQ(root->children_.at('t')->children_.at('e')->children_.size(), 1);
  ASSERT_EQ(root->children_.at('t')->children_.at('e')->children_.at('s')->children_.size(), 1);
  ASSERT_EQ(root->children_.at('t')->children_.at('e')->children_.at('s')->children_.at('t')->children_.size(), 0);
  ASSERT_TRUE(root->children_.at('t')->children_.at('e')->children_.at('s')->children_.at('t')->is_value_node_);
}
}
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
