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

#include <fstream>
#include <cstdlib>

// int main()
// {
//     std::string s = "Danm it!!";
//     TreeNode root(s, 5);
//     std::cout<<root.getWord()<<std::endl;
//     return 0;
// }

void consoleApp()
{
    //TODO:
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
        // std::cout<<"constructing node"<<std::endl;
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
    return pos.get(occur-1);
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
    ++l;
}

/*
 * Doesn't do range check (as of now), need to make sure that the access index
 * is IN RANGE!
 * Return -1 if out of range
 */
int LinkedList::get(int idx) const
{
    // TODO: add exception for out of range indexing
    LinkedNode *pt = head;
    for (int i = 0; i < idx; ++i)
    {
        if (pt)
        {
            pt = pt->next;
        } else
        {
            return -1;
        };
    }

    if (pt)
    {
        return pt->pos;
    } else
    {
        return -1;
    }
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

bool TreeNode::contains(int pos) const
{
    return this->pos.length() >= pos;
}

int LinkedList::length() const
{
    return l;
}

void BTree::print() const
{
    if (root)
    {
         print(root);
     } else
    {
        std::cout<<"The tree is empty";
    }
    std::cout<<std::endl;

}

void BTree::print(TreeNode *node) const
{
    std::cout<<node->word;
       std::cout<<"(";

    if (node->left)
    {
         print(node->left);
     } else
    {
        std::cout<<".";
    }

    if (node->right)
    {
         print(node->right);
     } else
    {
        std::cout<<".";
    }
       std::cout<<")";

}

void parser(BTree &tree, std::string fileName)
{
    // std::string s("sixpence.txt");
    std::ifstream inf(fileName.c_str());

    std::ofstream outf("test_result.txt");


    if (!inf)
    {
        std::cerr<<"File test_result.txt could not be opened!"<<std::endl;
        exit(1);
    }
    char* buffer = new char[256]; // may use precessor constant for the length
    int length = 256;
    // use 2 pointers to indicating read extend
    int ptrs = 0;
    int ptre = ptrs;
    int count = 0;          // word count
    while(inf)
    {
        // read in to buffer
        inf.read(buffer+ptrs, length-ptrs);
        std::cout << "Reading " << length-ptrs << " characters." <<std::endl;

        if (!inf)
        {
            length = inf.gcount();
        }

        bool reachEnd = false;

        ptrs = 0;
        ptre = ptrs;
        // Read one word from the buffer
        while (!reachEnd){
            ptrs = ptre;

            while(!validChar(buffer[ptrs]))
            {
                ++ptrs;
                if (ptre >= length)
                {
                    reachEnd = true;
                }
            }
            if (!reachEnd)
            {
                ptre = ptrs;
                while(validChar(buffer[ptre]))
                {
                    ++ptre;
                    if (ptre >= length)
                    {
                        reachEnd = true;
                    }
                }
                if (reachEnd)
                {
                    // if there is still word at the end of the buffer, copy it to the beginning of the buffer
                    ptre = ptrs;
                    ptrs = 0;
                    while (ptre < length)
                    {
                        buffer[ptrs] = buffer[ptre];
                        ++ptrs;
                        ++ptre;
                    }
                }else
                {
                    buffer[ptre] = '\0';    // Mark the end of the word
                    ++ count;
                    tree.insert(std::string(buffer+ptrs), count);
                        //outf <<"    "<<std::string(buffer+ptrs) <<std::endl;
                }
            }

        }



    }
    if (ptrs > 0)
    {
        buffer[ptrs] = '\0';
        ++count;
        tree.insert(std::string(buffer), count);
    }
    inf.close();
    delete[] buffer;
}


bool validChar(char c)
{
    return (c<='z' && c >= 'a') || (c<='Z' && c >= 'A') || (c<='9' && c >= '0') || (c == '\'');
}
