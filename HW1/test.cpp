#include <iostream>
#include "wl.h"

int main()
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
    std::cout<<t.search("Qisi")->locate(49)<<std::endl;
    std::cout<<t.search("Qisi")->locate(2)<<std::endl;


}
