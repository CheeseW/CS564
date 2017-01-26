//
// File: wl.h
//
//  Description: Add stuff here ...
//  Student Name: Add stuff here ..
//  UW Campus ID: Add stuff here ..
//  enamil: Add stuff here ..

/*
 * @mainpage Homework1 - Word Locator in C++
 */

/**
 * @file wl.cpp
 * @author Qisi Wang
 */

#include "wl.h"


int main()
{
    std::string s = "Danm it!!";
    TreeNode root(s, 5);
    std::cout<<root.getWord()<<std::endl;
    return 0;
}

LinkedNode::LinkedNode(int pos):
    pos(pos),
    next(NULL)
{}

TreeNode::TreeNode(std::string word, int pos):
    left(NULL),
    right(NULL),
    word(word)
    {
        this->pos.append(pos);
    }

std::string TreeNode::getWord() const
{
    return word;
}


BTree::~BTree()
{
    destroyTree(root);
}

void BTree::insert(std::string key, int pos)
{
        if (root)
    {
        insert(key, pos, root);
    }
    else
    {
        root = new TreeNode(key, pos);
    }
}

TreeNode* BTree::search(std::string key) const
{
    return search(key, root);
}

void BTree::clear()
{
        destroyTree(root);
        root = NULL;
}

void BTree::insert(std::string key, int pos, TreeNode* node)
{
// node is garanteed to be not NULL
    if (key.compare(node->word) > 0)
    {
        if (node->right)
        {
            insert(key, pos, node->right);
        } else
        {
            node->right = new TreeNode(key, pos);
        }
    } else if (key.compare(node->word) < 0)
    {
        if (node->left)
        {
            insert(key, pos, node->left);
        } else
        {
            node->left = new TreeNode(key, pos);
        }
    } else
    {
        node->insert(pos);
    }
}

void TreeNode::insert(int pos)
{
    this->pos.append(pos);
}

TreeNode* BTree::search(std::string key, TreeNode* node) const
{
    if (node)
    {
        if (key.compare(node->word) < 0)
        {
            return search(key, node->left);
        } else if (key.compare(node->word) > 0)
        {
            return search(key, node->right);
        } else
        {
            return node;
        }
    } else
    {
        return NULL;
    }
}


void BTree::destroyTree(TreeNode* node)
{
    //Need to deal with NULL root
    if (node)
    {
        destroyTree(node->left);
        destroyTree(node->right);
        delete node;
    }
}

int TreeNode::locate(int occur)
{
    //TODO
    return 0;
}

LinkedList::~LinkedList()
{
    clear();
}

void LinkedList::append(int n)
{
    if (tail)
    {
        tail->next = new LinkedNode(n);
        tail = tail->next;
    } else
    {
        head = new LinkedNode(n);
        tail = head;
    }
}

/*
 * Return NULL if idx is out of range
 * Also notice that the index start from 0
 */
LinkedNode* LinkedList::get(int idx) const
{
    LinkedNode *pt = head;
    for (int i = 0; i < idx; ++i)
    {
        if (pt)
        {
            pt = pt->next;
        } else
        {
            return NULL;
        };
    }
    return pt;
    // if (pt)
    // {
    //     return pt->pos;
    // } else
    // {
    //     return NULL;
    // }

}

void LinkedList::clear()
{
    while(head)
    {
        tail = head->next;
        delete head;
        head = tail;
    }
}


void testing()
{
    //TOOD
}
