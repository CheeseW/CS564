// could be member fucntions or standalone functions
/** 
   * function added by yanqi CS564
   * used to insert & sort & compact the data in a leaf node
   * caller is responsible for making sure that the leaf page is not overflowed
   * @param node: the leaf node to be updated
   * @param key: the key to be inserted
   * @param rid: the RecordId to be inserted
   **/
  void leafNodeInsert(LeafNodeInt& node, int key, RecordId rid);

  /** 
   * function added by yanqi CS564
   * used to split & insert & sort & compact the data in a leaf node
   * caller is responsible for making sure that the leaf page is overflowed when adding a new (key, rid) tuple
   * @param oriNode: the original leaf node
   * @param newNode: the newly allocated leaf node
   * @param updateTuple: contains 2 pid + 1 key for upper level updates
   */
  void leafNodeInsertSplit(int key, RecordId rid, LeafNodeInt& oriNode, LeafNodeInt& newNode, PageTwoKeyTuple& updateTuple);


  /** 
   * function added by yanqi CS564
   * used to insert & sort & compact the data in a leaf node
   * caller is responsible for making sure that the leaf page is not overflowed
   * @param node: the non-leaf node to be updated
   * @param updateTuple: the information used to update the current node
   */
  void nonLeafNodeInsert(NonLeafNodeInt& node, const PageTwoKeyTuple& updateTuple);

   /** 
   * function added by yanqi CS564
   * used to split & insert & sort & compact the data in a leaf node
   * caller is responsible for making sure that the leaf page is overflowed by adding a new (key, rid) tuple
   * @param oriNode: the original non-leaf node
   * @param newNode: the newly allocated non-leaf node
   * @param updateTuple: key and 2 pid used to update the current node
   * @param feedbackTuple: the feedback information used to update the upper levels
   */
  void nonLeafNodeInsert(NonLeafNodeInt& oriNode, NonLeafNodeInt& newNode, const PageTwoKeyTuple& updateTuple, PageTwoKeyTuple& feedbackTuple);