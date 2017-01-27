#include <iostream>
#include <fstream>
#include <cstdlib>
#include "wl.h"

void dtTest();
void fioTest();
void parserTest();
bool validChar(char a);


int main()
{
    BTree t;
    parser(t, "sixpence.txt");
}

/**
 * Testing the BST and Linkedlist used for the homework
 */
void dtTest()
{
    std::string s= "testing";
    std::cout << s << std::endl;

    BTree t;
    t.insert("I",1);
    t.insert("am",2);
    t.insert("Qisi",3);
    t.print();
    std::cout<<t.search("I")->locate(1)<<std::endl;
    std::cout<<t.search("am")->locate(1)<<std::endl;
    std::cout<<t.search("Qisi")->locate(1)<<std::endl;
    t.insert("Qisi",199);
    std::cout<<t.search("Qisi")->locate(400)<<std::endl;
    std::cout<<t.search("Qisi")->locate(2)<<std::endl;
}


void fioTest()
{
    std::string s("sixpence.txt");
    std::ifstream inf(s.c_str());

    std::ofstream outf("test_result.txt");
    //std::ifstream inf("sixpence.txt");

    if (!inf)
    {
        std::cerr<<"File test_result.txt could not be opened!"<<std::endl;
        exit(1);
    }
    while(inf)
    {
        std::string strInput;
        inf >> strInput;
        outf <<"    "<< strInput;
        // std::cout<<strInput<<std::endl;
    }
}

void parserTest()
{
    std::string s("sixpence.txt");
    std::ifstream inf(s.c_str());

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
        int count = 0;          // A varible for debug purposed to prevent inf loop TODO: delete
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
                    outf <<"    "<< std::string(buffer+ptrs)<<std::endl;
                }
            }

        }



    }
    if (ptrs > 0)
    {
        buffer[ptrs] = '\0';
        outf<< std::string(buffer)<<std::endl;
    }
    inf.close();
    delete[] buffer;
}
