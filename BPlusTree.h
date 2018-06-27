#pragma once

#ifndef __BplusTree__

#define __BplusTree__

#include <vector>
#include <stdio.h>
#include <cstring>
#include <string>
#include "BufferManager.h"

;using namespace std;

typedef int OffsetType;
extern Buffer* global_buffer;

union data_trans
{
	bool boolvar;
	int integer;
	float decimal;
	char character[256];
};


const int BLOCKSIZE = 4096;
const int INT_TYPE = 0;
const int FLOAT_TYPE = 1;
const int STRING_TYPE = 2;

#define MIN_NUM ((degree-1)/2)
#define MAX_NUM degree
typedef int OffsetType;
typedef int IndexType;


template <class KeyType>
class Node
{
public:
	int key_num;
	bool IsLeaf;
	int key_size;

	OffsetType Parent;
	OffsetType Self;
	OffsetType NextNode;
	OffsetType PrevNode;

	vector<KeyType> 	KeySet;
	vector<OffsetType> 	OffsetSet;

	Node();
	Node(int m_key_size);
	~Node();

	int Search_In_Node(KeyType Key);
	int Search_In_Node_exact(KeyType key);
	int Find_Data_Offset(KeyType key);
	bool Delete_from_Node(KeyType Key);
	int Insert_Into_Node(KeyType Key, OffsetType DataOffset);
	int Insert_Into_Node(KeyType Key);

	bool Search_In_Node_For_Delete(KeyType key, IndexType &index);
	bool RemoveAt(IndexType index);
};

template <class KeyType>
class BPlusTree
{
public:
	OffsetType root;
	OffsetType LeafHead;
	int key_type;
	int key_size;
	int degree;
	int level;
	string BPlusTree_name;

	BPlusTree();
	BPlusTree(string Name, int KeySize, int KType, int Degree);
	~BPlusTree();
	
	void Parse_Index(char * BlockContent);
	void Inver_ParseIndex(char * BlockContent);
	OffsetType Get_Leaf_Node(KeyType key, Node<KeyType> & TreeNode);
	OffsetType Get_Data_Address(KeyType key, Node<KeyType> & TreeNode);
	bool Insert(KeyType Key, OffsetType DataOffset);
	bool Struct_Adjust_Insert(Node<KeyType> & TreeNode);
	OffsetType splite(Node<KeyType> & OldNode, Node<KeyType> & NewNode, KeyType & Key);
	bool Delete(KeyType Key);
	bool AdjustAfterDelete(Node<KeyType> & node);
	bool Search(KeyType Key, OffsetType & offset);//yyr

	void Level_List();//yyr

	void Bm_Delete_Node(Node<KeyType> &node);
	void UpdateNode(Node<KeyType> & node, OffsetType offset);
	void GetNode(Node<KeyType> & node, OffsetType offset);
};

/******************************************************************************************/

void ParseNode(char * BlockContent, Node<int> & TreeNode);
void ParseNode(char * BlockContent, Node<float> & TreeNode);
void ParseNode(char * BlockContent, Node<string> & TreeNode);
void Inver_ParseNode(char * BlockContent, Node<int> & TreeNode);
void Inver_ParseNode(char * BlockContent, Node<float> & TreeNode);
void Inver_ParseNode(char * BlockContent, Node<string> & TreeNode);

template <class KeyType>
void UpdateIndex(BPlusTree<KeyType> * BPT)
{
	char Content[4096];

	BPT->Inver_ParseIndex(Content);
    global_buffer->updateBlock(BPT->BPlusTree_name, Content, 0);
}



template <class KeyType>
void GetIndexHead(BPlusTree<KeyType> * BPT)
{
	blockNode * BN;
	char * Content;
    
	BN = global_buffer->getBlockByOffset(BPT->BPlusTree_name, 0);
	Content = global_buffer->getContent(BN);
	BPT->Parse_Index(Content);
}


/******************************************************************************************/


template <class KeyType>
int Node<KeyType>::Search_In_Node(KeyType Key)
{

	if (key_num == 0)
		return 0;

	int index = -1;


	if (Key > KeySet[key_num - 1])
		return key_num;
	if (Key < KeySet[0])
		return 0;

	int left = 0, right = key_num - 1, pos = 0;
	while (right > left + 1)
	{
		pos = (right + left) / 2;
		if (KeySet[pos] == Key)
		{
			return pos ;
		}
		else if (KeySet[pos] < Key)
			left = pos;
		else
			right = pos;
	}

	if (KeySet[left] >= Key)
		index = left;
	else if (KeySet[right] >= Key)
		index = right;
	else if (KeySet[right] < Key)
		index = right + 1;

	return index;
}


template <class KeyType>
int Node<KeyType>::Search_In_Node_exact(KeyType key)
{
	if (key_num <= 0)
		return -1;

	int mid, left, right;
	right = key_num-1;
	left = 0;
	while (left <= right)
	{
		mid = (right + left) / 2;
		if (key < KeySet[mid])
			right = mid - 1;
		else if (key > KeySet[mid])
			left = mid + 1;
		else
			return mid;
	}
	return -1;
}


template <class KeyType>
int Node<KeyType>::Find_Data_Offset(KeyType key)
{
	if (key_num <= 0)
		return -1;

	int mid, left, right;
	right = key_num-1;
	left = 0;
	while (left <= right)
	{
		mid = (right + left) / 2;
		if (key < KeySet[mid])
			right = mid - 1;
		else if (key > KeySet[mid])
			left = mid + 1;
		else
			return mid;
	}
	return -1;
}

template <class KeyType>
int Node<KeyType>::Insert_Into_Node(KeyType Key, OffsetType DataOffset)
{
	int index;

	if (!IsLeaf)
		return -1;

	if (key_num == 0)
	{
		KeySet[0] = Key;
		OffsetSet[0] = DataOffset;
		key_num++;
		index = 0;
	}
	else
	{
		index = Search_In_Node(Key);
		int exist = Search_In_Node_exact(Key);
		if(exist >= 0)
		{
//            cout << "Error:In add(Keytype &key, offsetNumber val),key has already in the tree!" << endl;
			return -1;
		}
		
		KeySet.insert(KeySet.begin() + index, Key);
		OffsetSet.insert(OffsetSet.begin() + index, DataOffset);
	
		key_num++;
	}

	return index;
}


template <class KeyType>
int Node<KeyType>::Insert_Into_Node(KeyType Key)
{
	int index, i;

	if (IsLeaf)
		return -1;

	if (key_num == 0)
	{
		KeySet[0] = Key;
		key_num++;
		index = 0;
	}
	else
	{
		index = Search_In_Node(Key);
		int exist = Search_In_Node_exact(Key);
		if (exist >= 0)
		{
//            cout << "Error:In add(Keytype &key, offsetNumber val),key has already in the tree!" << endl;
			return -1;
		}

		KeySet.push_back(Key);
		for (i = key_num; i > index; i--)   //different from demo !!!!!!!!!!!!!!!!!!!!
			KeySet[i] = KeySet[i - 1];
		KeySet[index] = Key;

		OffsetSet.push_back(OffsetType());
		for (i = key_num + 1; i > index + 1; i--)
			OffsetSet[i] = OffsetSet[i - 1];
		OffsetSet[index + 1] = -1;
		key_num++;
	}

	return index;
}


/*********************************************************************************************/

template <class KeyType>
OffsetType BPlusTree<KeyType>::splite(Node<KeyType> & OldNode, Node<KeyType> & NewNode, KeyType & Key)
{
	int min_node = (degree - 1) / 2;
	int i;
	Node<KeyType> ChildNode(key_size);

	if (OldNode.IsLeaf)
	{
		Key = OldNode.KeySet[min_node + 1];
		for (i = min_node+1; i<degree; i++)
		{
			NewNode.KeySet.insert(NewNode.KeySet.begin() + (i - min_node - 1), OldNode.KeySet[i]);
			OldNode.KeySet[i] = KeyType();
			NewNode.OffsetSet.insert(NewNode.OffsetSet.begin() + (i - min_node - 1), OldNode.OffsetSet[i]);
			OldNode.OffsetSet[i] = -1;
		}

		NewNode.NextNode = OldNode.NextNode;
		NewNode.PrevNode = OldNode.Self;
		OldNode.NextNode = NewNode.Self;
		if (NewNode.NextNode != -1)
		{
			GetNode(ChildNode, NewNode.NextNode);
			ChildNode.PrevNode = NewNode.Self;
			UpdateNode(ChildNode, NewNode.NextNode);
		}

		NewNode.Parent = OldNode.Parent;
		NewNode.key_num = min_node;
		OldNode.key_num = min_node + 1;
		NewNode.IsLeaf = true;
	}
	else
	{
		Key = OldNode.KeySet[min_node];
		for (i = min_node + 1; i<degree + 1; i++)
		{
			NewNode.OffsetSet.insert(NewNode.OffsetSet.begin() + (i - min_node - 1), OldNode.OffsetSet[i]);
			GetNode(ChildNode, OldNode.OffsetSet[i]);
			ChildNode.Parent = NewNode.Self;
			UpdateNode(ChildNode, OldNode.OffsetSet[i]);

			OldNode.OffsetSet[i] = -1;
		}
		for (i = min_node+1; i<degree+1; i++)
		{
			NewNode.KeySet.insert(NewNode.KeySet.begin() + (i - min_node - 1), OldNode.KeySet[i]);;
			OldNode.KeySet[i] = KeyType();
		}

		NewNode.NextNode = OldNode.NextNode;
		NewNode.PrevNode = OldNode.Self;
		OldNode.NextNode = NewNode.Self;
		if (NewNode.NextNode != -1)
		{
			GetNode(ChildNode, NewNode.NextNode);
			ChildNode.PrevNode = NewNode.Self;
			UpdateNode(ChildNode, NewNode.NextNode);
		}
		

		NewNode.Parent = OldNode.Parent;
		OldNode.key_num = min_node;
		NewNode.key_num = min_node;
	}

	return NewNode.Self;
}






/*********************************************************************************************/






template <class KeyType>
bool BPlusTree<KeyType>::Insert(KeyType Key, OffsetType DataOffset)
{
	Node<KeyType> Root(key_size), TargetLeaf(key_size);
	OffsetType LeafOffset;
	int index;

	GetNode(Root, root);
	LeafOffset = Get_Leaf_Node(Key, Root);
	UpdateNode(Root, root);
	GetNode(TargetLeaf, LeafOffset);
	
	index = TargetLeaf.Search_In_Node_exact(Key);
	if (index > 0)
	{
//        cout << "Error:in insert key to index: the duplicated key!" << endl;
		return false;
	}
	else
	{
		TargetLeaf.Insert_Into_Node(Key, DataOffset);
		UpdateNode(TargetLeaf, TargetLeaf.Self);
		if (TargetLeaf.key_num == degree)
			Struct_Adjust_Insert(TargetLeaf);

		return true;
	}

}



template <class KeyType>
OffsetType BPlusTree<KeyType>::Get_Data_Address(KeyType key, Node<KeyType> & TreeNode)
{
	int DataOffset;
	int index = -1;

	if (!TreeNode.IsLeaf)
		return -1;

	index = TreeNode.Find_Data_Offset(key);
	if (index < 0)
		DataOffset = -1;
	else
		DataOffset = TreeNode.OffsetSet[index];

	return DataOffset;
}


template <class KeyType>
OffsetType BPlusTree<KeyType>::Get_Leaf_Node(KeyType key, Node<KeyType> & TreeNode)
{
	int index;
	OffsetType LeafOffset, NewOffset;

	if (TreeNode.IsLeaf)
		return TreeNode.Self;
	else
	{
		index = TreeNode.Search_In_Node(key);
		NewOffset = TreeNode.OffsetSet[index];
		Node<KeyType> Child(key_size);
		GetNode(Child, NewOffset);
		UpdateNode(Child, NewOffset);
		LeafOffset = Get_Leaf_Node(key, Child);
	}

	return LeafOffset;
}






template <class KeyType>
void BPlusTree<KeyType>::Parse_Index(char * BlockContent)
{
	data_trans temp;
	char temp_key[100];
	int UsingSize = 0;
    
    memcpy(temp_key, BlockContent + UsingSize, 100);
    BPlusTree_name = temp_key;
    UsingSize += 100;
    
	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	root = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	LeafHead = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	key_type = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	key_size = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	degree = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	level = temp.integer;

	

}

template <class KeyType>
void BPlusTree<KeyType>::Inver_ParseIndex(char * BlockContent)
{
	data_trans temp;
	char temp_key[100];
	int UsingSize = 0;
    
    strcpy(temp_key, BPlusTree_name.c_str());
    memcpy(BlockContent + UsingSize, temp_key, 100);
    UsingSize += 100;
    
	temp.integer = root;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = LeafHead;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = key_type;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	temp.integer = key_size;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	temp.integer = degree;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	temp.integer = level;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	
}


//CONSTRUCTOR NAD DESTRUCTOR

template <class KeyType>
Node<KeyType>::Node()
{
	KeySet.push_back(KeyType());
	OffsetSet.push_back(int());
	key_num = 0;
	key_size = 4;
	IsLeaf = false;
	Parent = NextNode = PrevNode = Self = -1;
}


template <class KeyType>
Node<KeyType>::Node(int m_key_size)
{
	KeySet.push_back(KeyType());
	OffsetSet.push_back(int());
	key_num = 0;
	key_size = m_key_size;
	IsLeaf = false;
	Parent = NextNode = PrevNode = Self = -1;
}

template <class KeyType>
Node<KeyType>::~Node()
{

}


template <class KeyType>
BPlusTree<KeyType>::BPlusTree()
{
	root = 1;
	LeafHead = 1;
	key_type = 3;
	key_size = 4;
	degree = 5;
	level = 0;
}

template <class KeyType>
BPlusTree<KeyType>::BPlusTree(string Name, int KeySize, int KType, int Degree)
{
	BPlusTree_name = Name;
	key_size = KeySize;
	key_type = KType;
	degree = Degree;
	root = 1;
	LeafHead = 1;
	level = 0;
}

template <class KeyType>
BPlusTree<KeyType>::~BPlusTree()
{

}

/*******************************************************************************************************************************************************************/
/**/

template <class KeyType>
bool Node<KeyType>::Delete_from_Node(KeyType Key)
{
	IndexType pos;
	bool IsFind = Search_In_Node_For_Delete(Key, pos);
	if (IsFind)
	{
		if (IsLeaf)
		{

			key_num--;
			KeySet.erase(KeySet.begin() + pos);
			OffsetSet.erase(OffsetSet.begin() + pos);

			return true;

		}
		else
		{
			key_num--;
			KeySet.erase(KeySet.begin() + pos);
			OffsetSet.erase(OffsetSet.begin() + pos + 1);
		}
	}
	else
	{
//        cout << "The key is not in the list." << endl;
		return false;
	}
}
template <class KeyType>
bool Node<KeyType>::Search_In_Node_For_Delete(KeyType key, IndexType &index)
{
	index = -1;
	if (key_num == 0)
	{
		index = 0;
		return false;
	}
	else
	{
		if (KeySet.at(key_num - 1) < key)
		{
			index = key_num;
			return false;
		}
		else if (KeySet.at(0) > key)
		{
			index = 0;
			return false;
		}
		else
		{
			for (int i = 0; i < key_num; ++i)
			{
				if (key == KeySet.at(i))
				{
					index = i;
					return true;
				}
				else if (KeySet.at(i) < key)
				{
					continue;
				}
				else if (KeySet.at(i) > key)
				{
					index = i;
					return false;
				}
			}
		}
	}
	return false;
}
template <class KeyType>
bool Node<KeyType>::RemoveAt(IndexType index)//移除第index个值右侧
{
	if (index > key_num)
	{
//        cout << " Error, index is more than numbers of key";
		return false;
	}
	else
	{
		if (IsLeaf)
		{
			KeySet.erase(KeySet.begin() + index);
			OffsetSet.erase(OffsetSet.begin() + index);
			
			key_num -= 1;
		}
		else
		{
			KeySet.erase(KeySet.begin() + index);
			OffsetSet.erase(OffsetSet.begin() + index + 1);
			//OffsetSet.push_back(OffsetSet.at(0));
			OffsetSet[key_num] = -1;
			key_num -= 1;
		}

		return true;
	}
}

template <class KeyType>
bool BPlusTree<KeyType>::Delete(KeyType Key)
{
	OffsetType searchNodeOffset = -1;
	bool IsFind;
	IsFind = Search(Key, searchNodeOffset);
    if (false == IsFind) {
//        cout << "Error, there is no Key in the tree" << endl;
        return false;
    }
	else
	{
		Node<KeyType> searchNode;
		GetNode(searchNode, searchNodeOffset);
		IndexType index;
		bool IsFind;
		IsFind = searchNode.Search_In_Node_For_Delete(Key, index);

		if (root == searchNode.Self)
		{
			searchNode.RemoveAt(index);
			UpdateNode(searchNode, searchNode.IsLeaf);
			return AdjustAfterDelete(searchNode);
		}
		else
		{
			if (index == 0 && LeafHead != searchNode.Self)//branch
			{
				IndexType parentIndex;
				Node<KeyType> parentNode;
				GetNode(parentNode, searchNode.Parent);
				bool is_find_in_branch = parentNode.Search_In_Node_For_Delete(Key, parentIndex);
				while (false == is_find_in_branch)
				{
					if (parentNode.Parent != -1)
					{
						GetNode(parentNode, parentNode.Parent);
					}
					else
					{
						break;
					}
					is_find_in_branch = parentNode.Search_In_Node_For_Delete(Key, parentIndex);
				}
				parentNode.KeySet[parentIndex] = searchNode.KeySet.at(1);
				UpdateNode(parentNode, parentNode.Self);
				searchNode.RemoveAt(index);
				UpdateNode(searchNode, searchNode.Self);
				return AdjustAfterDelete(searchNode);
			}
			else//just leaf
			{
				searchNode.RemoveAt(index);
				UpdateNode(searchNode, searchNode.Self);
				return AdjustAfterDelete(searchNode);
			}
		}
	}
}

template <class KeyType>
bool BPlusTree<KeyType>::AdjustAfterDelete(Node<KeyType> & curNode)
{
	Node<KeyType> parentNode;
	Node<KeyType> brotherNode;
	Node<KeyType> leftNode, rightNode;
	Node<KeyType> grandsonNode;
	Node<KeyType> rootNode;
	Node<KeyType> sonNode;
	
	IndexType index = 0;
//    IndexType branch_index = 0;
	//if (((curNode.IsLeaf) && (curNode.key_num >= MIN_NUM)) || ((degree != 3) && (!curNode.IsLeaf) && (curNode.key_num >= MIN_NUM - 1)) || ((degree == 3) && (!curNode.IsLeaf) && (curNode.key_num < 0)))
	if (curNode.key_num >= MIN_NUM)
	{
		return true;
	}
	if (curNode.Self == root)
	{
		if (curNode.key_num > 0)
		{
			return true;
		}
		else
		{

			GetNode(rootNode, root);
			if (rootNode.IsLeaf)
			{
				
				LeafHead = root;
				rootNode.NextNode = -1;
				UpdateNode(rootNode, rootNode.Self);
			}
			else
			{

				root = curNode.OffsetSet.at(0);
				GetNode(rootNode, root);
				rootNode.Parent = root;
				Bm_Delete_Node(curNode);
				UpdateNode(rootNode, rootNode.Self);

			}
			return true;
		}

	}//end root
	else
	{

		GetNode(parentNode, curNode.Parent);
		if (curNode.IsLeaf)
		{
			index = 0;
			parentNode.Search_In_Node_For_Delete(curNode.KeySet[0], index);
			if ((parentNode.OffsetSet[0] != curNode.Self) && (index + 1 == parentNode.key_num))//curnode is rightest. choose the left to merge or add
			{
				GetNode(brotherNode, parentNode.OffsetSet[index]);
				if (brotherNode.key_num > MIN_NUM)
				{
					//GetNode(sonNode, brotherNode.OffsetSet.at(brotherNode.key_num - 1));
					//sonNode.Parent = curNode.Self;
					//UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

					curNode.KeySet.insert(curNode.KeySet.begin(), brotherNode.KeySet.at(brotherNode.key_num - 1));
					curNode.OffsetSet.insert(curNode.OffsetSet.begin(), brotherNode.OffsetSet.at(brotherNode.key_num - 1));
					//curNode.OffsetSet.push_back(brotherNode.OffsetSet[0]);

					curNode.key_num += 1;
					brotherNode.RemoveAt(brotherNode.key_num - 1);
					parentNode.KeySet[index] = curNode.KeySet[0];
					UpdateNode(brotherNode, brotherNode.Self);
					UpdateNode(parentNode, parentNode.Self);
					UpdateNode(curNode, curNode.Self);
					return true;
					// curNode.KeySet[0].push_back(brotherNode.KeySet.);

				}//end add
				else//merge with left brother and free curnode
				{
					parentNode.RemoveAt(index);


					for (int i = 0; i < curNode.key_num; ++i)
					{
						/*          brotherNode.KeySet.push_back(curNode.KeySet[i]);
						brotherNode.OffsetSet.push_back(curNode.OffsetSet[i]);*/
						//GetNode(sonNode, curNode.OffsetSet[i]);
						//sonNode.Parent = brotherNode.Self;
						//UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

						brotherNode.KeySet.insert(brotherNode.KeySet.begin() + brotherNode.key_num + i, curNode.KeySet[i]);
						brotherNode.OffsetSet.insert(brotherNode.OffsetSet.begin() + brotherNode.key_num + i, curNode.OffsetSet[i]);
					}

					brotherNode.key_num += curNode.key_num;
					brotherNode.NextNode = curNode.NextNode;

					UpdateNode(brotherNode, brotherNode.Self);
					UpdateNode(parentNode, parentNode.Self);
					UpdateNode(curNode, curNode.Self);


					Bm_Delete_Node(curNode);
					return AdjustAfterDelete(parentNode);
				}//end merge           

			}//end left
			else//choose  right
			{
				if (parentNode.OffsetSet[0] == curNode.Self)
				{
					GetNode(brotherNode, parentNode.OffsetSet[1]);
				}
				else
				{
					GetNode(brotherNode, parentNode.OffsetSet[index + 2]);
				}

				if (brotherNode.key_num > MIN_NUM)//choose left key add
				{
					/*  curNode.KeySet.push_back(brotherNode.KeySet[0]);
					curNode.OffsetSet.push_back(brotherNode.OffsetSet[0]);*/
					//GetNode(sonNode, brotherNode.OffsetSet[0]);
					//sonNode.Parent = curNode.Self;
					//UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

					curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, brotherNode.KeySet[0]);
					curNode.OffsetSet.insert(curNode.OffsetSet.begin() + curNode.key_num, brotherNode.OffsetSet[0]);

					curNode.key_num++;
					brotherNode.RemoveAt(0);
					if (parentNode.OffsetSet[0] == curNode.Self)
					{
						parentNode.KeySet[0] = brotherNode.KeySet[0];
					}
					else
					{
						parentNode.KeySet[index + 1] = brotherNode.KeySet[0];
					}
					UpdateNode(curNode, curNode.Self);
					UpdateNode(parentNode, parentNode.Self);
					UpdateNode(brotherNode, brotherNode.Self);

					return true;
				}//end add
				else//merge
				{
					for (int i = 0; i < brotherNode.key_num; ++i)
					{
						/*curNode.KeySet.push_back(brotherNode.KeySet[i]);
						curNode.OffsetSet.push_back(brotherNode.OffsetSet[i]);*/
						//GetNode(sonNode, brotherNode.OffsetSet[i]);
						//sonNode.Parent = curNode.Self;
						//UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

						curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num + i, brotherNode.KeySet[i]);
						curNode.OffsetSet.insert(curNode.OffsetSet.begin() + curNode.key_num + i, brotherNode.OffsetSet[i]);

					}
					if (curNode.Self == parentNode.OffsetSet[0])
					{
						parentNode.RemoveAt(0);
					}
					else
					{
						parentNode.RemoveAt(index + 1);
					}
					UpdateNode(parentNode, parentNode.Self);

					curNode.key_num += brotherNode.key_num;
					curNode.NextNode = brotherNode.NextNode;
					UpdateNode(curNode, curNode.Self);
					
					if (brotherNode.NextNode != -1)
					{
						GetNode(rightNode, brotherNode.NextNode);
						rightNode.PrevNode = curNode.Self;
						UpdateNode(rightNode, rightNode.Self);
					}
										
					Bm_Delete_Node(brotherNode);
					return AdjustAfterDelete(parentNode);
				}//end merge right to the left(curnode)



			}

		}//end leaf

		else//branch
		{
			index = 0;
			GetNode(grandsonNode, curNode.OffsetSet[0]);
			
			parentNode.Search_In_Node_For_Delete(grandsonNode.KeySet.at(0), index);
			if ((parentNode.OffsetSet[0] != curNode.Self) && (index + 1 == parentNode.key_num))//choose left brother
			{
				GetNode(brotherNode, parentNode.OffsetSet[index]);
				if (brotherNode.key_num > MIN_NUM)//most right key add to curnode
				{
					GetNode(sonNode, brotherNode.OffsetSet[index + 1]);
					sonNode.Parent = curNode.Self;
					UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

					curNode.KeySet.insert(curNode.KeySet.begin(), brotherNode.KeySet[index]);
					curNode.OffsetSet.insert(curNode.OffsetSet.begin(), brotherNode.OffsetSet[index+1]);
					curNode.key_num++;		

					parentNode.KeySet[index] = brotherNode.KeySet[brotherNode.key_num - 1];

					if (brotherNode.OffsetSet[brotherNode.key_num] != -1)
					{
						GetNode(grandsonNode, brotherNode.OffsetSet[brotherNode.key_num]);
						grandsonNode.Parent = curNode.Self;
						UpdateNode(grandsonNode, grandsonNode.Self);
					}
					brotherNode.RemoveAt(brotherNode.key_num - 1);

					UpdateNode(curNode, curNode.Self);
					UpdateNode(brotherNode, brotherNode.Self);
					UpdateNode(parentNode, parentNode.Self);

				}//end add
				else//merge
				{
					//brotherNode.KeySet.push_back(parentNode.KeySet[index]);
					brotherNode.KeySet.insert(brotherNode.KeySet.begin() + brotherNode.key_num, parentNode.KeySet[index]);
					parentNode.RemoveAt(index);
					brotherNode.key_num++;
					for (int i = 0; i < curNode.key_num; ++i)
					{
		
						GetNode(sonNode, curNode.OffsetSet[i]);
						sonNode.Parent = brotherNode.Self;
						UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

						brotherNode.KeySet.insert(brotherNode.KeySet.begin() + brotherNode.key_num + i, curNode.KeySet[i]);
						brotherNode.OffsetSet.insert(brotherNode.OffsetSet.begin() + brotherNode.key_num + i, curNode.OffsetSet[i]);
				
					}
					
					GetNode(sonNode, curNode.OffsetSet.at(curNode.key_num));
					sonNode.Parent = brotherNode.Self;
					UpdateNode(sonNode, sonNode.Self);//更新儿子的父亲节点

					brotherNode.OffsetSet.insert(brotherNode.OffsetSet.begin() + brotherNode.key_num + curNode.key_num, curNode.OffsetSet.at(curNode.key_num));

					brotherNode.key_num += curNode.key_num;
					brotherNode.NextNode = curNode.NextNode;

					brotherNode.NextNode = -1;//更新NextNode
					

					UpdateNode(brotherNode, brotherNode.Self);
					UpdateNode(parentNode, parentNode.Self);

					Bm_Delete_Node(curNode);
					return AdjustAfterDelete(parentNode);
				}
			}
			else//choose right
			{
				if (parentNode.OffsetSet[0] == curNode.Self)
				{
					GetNode(brotherNode, parentNode.OffsetSet[1]);
				}
				else
				{
					GetNode(brotherNode, parentNode.OffsetSet[index + 2]);
				}
				if (brotherNode.key_num > MIN_NUM)
				{
					/*curNode.OffsetSet.push_back(brotherNode.OffsetSet[0]);
					curNode.KeySet.push_back(brotherNode.KeySet[0]);*/
					curNode.OffsetSet.insert(curNode.OffsetSet.begin() + curNode.key_num + 1, brotherNode.OffsetSet[0]);
					if (curNode.Self == parentNode.OffsetSet[0])
					{
						curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, parentNode.KeySet[0]);
					}
					else
					{
						curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, parentNode.KeySet[index+1]);
					}
					
					

					GetNode(grandsonNode, brotherNode.OffsetSet[0]);
					grandsonNode.Parent = curNode.Self;
					UpdateNode(grandsonNode, grandsonNode.Self);

					curNode.key_num++;
					UpdateNode(curNode, curNode.Self);
/*********************************/
					//int temp_index = index;
					
					if (curNode.Self == parentNode.OffsetSet[0])
					{
						/*********/
						//curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, parentNode.KeySet[index]);
						parentNode.KeySet[0] = brotherNode.KeySet[0];
						UpdateNode(parentNode, parentNode.Self);						
					}
					else
					{
						//curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, parentNode.KeySet[index + 1]);
						parentNode.KeySet[index + 1] = brotherNode.KeySet[0];
						UpdateNode(parentNode, parentNode.Self);
					}

					UpdateNode(curNode, curNode.Self);

/*******************************************/
				/*	if (curNode.Self == parentNode.OffsetSet[0])
					{
						parentNode.KeySet[0] = brotherNode.KeySet[0];
						UpdateNode(parentNode, parentNode.Self);
					}
					else
					{
						parentNode.KeySet[index + 1] = brotherNode.KeySet[0];
						UpdateNode(parentNode, parentNode.Self);
					}*/
					brotherNode.OffsetSet[0] = brotherNode.OffsetSet[1];
					brotherNode.RemoveAt(0);
					UpdateNode(brotherNode, brotherNode.Self);
					return true;
				}
				else//merge
				{
					//curNode.KeySet.push_back(parentNode.KeySet[index]);
					
					if (curNode.Self == parentNode.OffsetSet[0])
					{
						curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, parentNode.KeySet[0]);
						parentNode.RemoveAt(0);
						UpdateNode(parentNode, parentNode.Self);
					}
					else
					{
						curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num, parentNode.KeySet[index + 1]);
						parentNode.RemoveAt(index + 1);
						UpdateNode(parentNode, parentNode.Self);
					}
					curNode.key_num++;
					for (int i = 0; i < brotherNode.key_num; ++i)
					{
						/*curNode.OffsetSet.push_back(brotherNode.OffsetSet[i]);
						curNode.KeySet.push_back(brotherNode.KeySet[i]);*/

						curNode.OffsetSet.insert(curNode.OffsetSet.begin() + curNode.key_num + i, brotherNode.OffsetSet[i]);
						curNode.KeySet.insert(curNode.KeySet.begin() + curNode.key_num + i, brotherNode.KeySet[i]);

						GetNode(grandsonNode, brotherNode.OffsetSet[i]);
						grandsonNode.Parent = curNode.Self;
						UpdateNode(grandsonNode, grandsonNode.Self);
					}
					//curNode.OffsetSet.push_back(brotherNode.OffsetSet[brotherNode.key_num]);
					curNode.OffsetSet.insert(curNode.OffsetSet.begin() + curNode.key_num + brotherNode.key_num, brotherNode.OffsetSet[brotherNode.key_num]);
					GetNode(grandsonNode, brotherNode.OffsetSet[brotherNode.key_num]);
					grandsonNode.Parent = curNode.Self;
					UpdateNode(grandsonNode, grandsonNode.Self);

					curNode.key_num += brotherNode.key_num;

					if (brotherNode.NextNode != -1)
					{
						GetNode(rightNode, brotherNode.NextNode);
						rightNode.PrevNode = curNode.Self;
						curNode.NextNode = brotherNode.NextNode;
						UpdateNode(rightNode, rightNode.Self);
					}
					else
					{
						curNode.NextNode = -1;
					}


					UpdateNode(brotherNode, brotherNode.Self);

					UpdateNode(curNode, curNode.Self);


					Bm_Delete_Node(brotherNode);
					return AdjustAfterDelete(parentNode);
				}
				

			}

		}
	}

	return false;

}



/**************************************************************************************************************************************************/


template <class KeyType>
bool BPlusTree<KeyType>::Search(KeyType Key, OffsetType & offset)
{
	Node<KeyType> curNode;
	Node<KeyType> rootNode, sonNode;
	bool IsEqual;
	GetNode(curNode, root);
	OffsetType index;
	if (curNode.key_num <= 0)
	{
		return false;
	}
	while (false == curNode.IsLeaf)
	{
		IsEqual = curNode.Search_In_Node_For_Delete(Key, index);
		if (IsEqual)
		{
			GetNode(curNode, curNode.OffsetSet[index + 1]);
		}
		else
		{
			GetNode(curNode, curNode.OffsetSet[index]);
		}

	}
	if (curNode.Search_In_Node_For_Delete(Key, index))
	{
		offset = curNode.Self;
		return true;
	}
	else
	{
		offset = curNode.Self;
		return false;
	}
}


template <typename KeyType>
void BPlusTree<KeyType>::Level_List()
{
	Node<KeyType> curNode, sonNode;
	Node<KeyType> leftNode, rightNode;
	GetNode(curNode, root);
	GetNode(sonNode, curNode.OffsetSet[0]);
	while (!curNode.IsLeaf)
	{
		GetNode(leftNode, curNode.Self);//the leftest node of the row
		while (curNode.NextNode != -1)
		{
			cout << curNode.Self << "[ ";
			for (int i = 0; i < curNode.key_num; ++i)
			{
				cout << curNode.KeySet[i] << " ";
			}
			cout << "] " << curNode.Parent<< ",  ";
			GetNode(curNode, curNode.NextNode);
		}
		//last node of each row
		cout << curNode.Self << "[ ";
		for (int i = 0; i < curNode.key_num; ++i)
		{
			cout << curNode.KeySet[i] << " ";
		}
		cout << "] "  << curNode.Parent << ",  " << endl;
		//next row
		GetNode(curNode, leftNode.OffsetSet[0]);
	}
	//if leaf head

	while (curNode.NextNode != -1)
	{
		cout << curNode.Self << "[ ";
		for (int i = 0; i < curNode.key_num; ++i)
		{
			cout << curNode.KeySet[i] << " ";
		}
		cout << "] " << curNode.Parent << ",  ";
		GetNode(curNode, curNode.NextNode);
	}
	//last node of  row
	cout << curNode.Self << "[ ";
	for (int i = 0; i < curNode.key_num; ++i)
	{
		cout << curNode.KeySet[i] << " ";
	}
	cout << "] " << curNode.Parent << ",  " << endl;
	cout << endl;

}



/***************************************************************************************************************************************************/
template<class KeyType>
bool BPlusTree<KeyType>::Struct_Adjust_Insert(Node<KeyType> & TreeNode)
{
	KeyType key;
	Node<KeyType> NewNode(key_size);
	//char * BlockContent;
	//char UpdateContent[4096];

	NewNode.Self = global_buffer->createNewBlock(BPlusTree_name);

	splite(TreeNode, NewNode, key);

	if (TreeNode.Self == root)
	{
		Node<KeyType> NewRoot(key_size);
		NewRoot.Self = global_buffer->createNewBlock(BPlusTree_name);
		level++;
		root = NewRoot.Self;
		TreeNode.Parent = root;
		NewNode.Parent = root;
		NewRoot.Insert_Into_Node(key);
		NewRoot.OffsetSet.insert(NewRoot.OffsetSet.begin(), TreeNode.Self);
		NewRoot.OffsetSet.insert(NewRoot.OffsetSet.begin() + 1, NewNode.Self);

		//BlockContent = GetIndexHead(BPlusTree_name,);
		//Inver_ParseIndex(UpdateContent);
		//UpdateIndex(UpdateContent, BPlusTree_name, this);

		UpdateIndex(this);

		UpdateNode(NewRoot, root);
		UpdateNode(NewNode, NewNode.Self);
		UpdateNode(TreeNode, TreeNode.Self);
	}
	else
	{
		Node<KeyType> parent(key_size);
		GetNode(parent, TreeNode.Parent);
		int index = parent.Insert_Into_Node(key);

		parent.OffsetSet[index+1] = NewNode.Self;
		NewNode.Parent = parent.Self;
		TreeNode.Parent = parent.Self;

		UpdateNode(NewNode, NewNode.Self);
		UpdateNode(TreeNode, TreeNode.Self);

		if (parent.key_num == degree)
			Struct_Adjust_Insert(parent);
		else
		{
			UpdateNode(parent, parent.Self);
		}
	}
	return true;
}


/************************************************************************************************************************************/

#endif

template <class KeyType>
void BPlusTree<KeyType>::UpdateNode(Node<KeyType> & node, OffsetType offset)
{
	char UpdateContent[4096];

	Inver_ParseNode(UpdateContent, node);
	global_buffer->updateBlock(BPlusTree_name, UpdateContent, offset);
}

template <class KeyType>
void BPlusTree<KeyType>::GetNode(Node<KeyType> & node, OffsetType offset)
{
	char * BlockContent;
	blockNode * BN;

	BN = global_buffer->getBlockByOffset(BPlusTree_name, offset);
	BlockContent = global_buffer->getContent(BN);
	ParseNode(BlockContent, node);
}

template <class KeyType>
void BPlusTree<KeyType>::Bm_Delete_Node(Node<KeyType> &node)
{
 	global_buffer->Delete_Block(BPlusTree_name, node.Self);
}
