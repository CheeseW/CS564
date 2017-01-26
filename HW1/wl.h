//
// File: wl.h
//
//  Description: Add stuff here ...
//  Student Name: Add stuff here ..
//  UW Campus ID: Add stuff here ..
//  email: Add stuff here ..

#ifndef WL_H
#define WL_H

#include <iostream>

class LinkedNode
{
private:
    int pos;
    LinkedNode *next;
public:
    LinkedNode(int pos);
    friend class LinkedList;
};

class LinkedList
{
private:
    LinkedNode* head;
    LinkedNode* tail;

public:
LinkedList(): head(NULL), tail(NULL) {}
    ~LinkedList();
    void append(int n);
    LinkedNode* get(int pos) const;
    void clear();

};



class TreeNode
{
private:
    TreeNode *left;
    TreeNode *right;

    std::string word;
    LinkedList pos;

public:
    TreeNode(std::string word, int pos);
    std::string getWord() const;
    int locate(int occur);
    void insert(int pos);
    friend class BTree;

};

class BTree
{
private:
    TreeNode *root;
public:
BTree():root(NULL){}
    ~BTree();
    void insert(std::string key, int pos);
    TreeNode* search(std::string key) const;
    void clear();

private:
    void insert(std::string key, int pos, TreeNode* node);
    TreeNode* search(std::string key, TreeNode* node) const;
    void destroyTree(TreeNode* node);
};


void testing();                 /* TODO: remove this method when submit */
#endif
