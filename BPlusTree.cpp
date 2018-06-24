#include "BPlusTree.h"

using namespace std;



OffsetType createNewBlock(string s)
{
	Buffer bf;
	//return bf.Create_Node();
	return bf.createNewBlock(s);
}



void ParseNode(char * BlockContent, Node<int> & TreeNode)
{
	data_trans temp;
	string temp_key;
	int i, j;
	int UsingSize = 0;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	TreeNode.key_num = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(bool));
	UsingSize += sizeof(bool);
	TreeNode.IsLeaf = temp.boolvar;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.Parent = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.Self = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.NextNode = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.PrevNode = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	TreeNode.key_size = temp.integer;

	j = 0;
	if (!TreeNode.IsLeaf)
	{
		memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
		//TreeNode.OffsetSet.push_back(temp.integer);
		TreeNode.OffsetSet.insert(TreeNode.OffsetSet.begin() + j, temp.integer);
		//TreeNode.OffsetSet[j++] = temp.integer;
		j++;
		UsingSize += sizeof(OffsetType);
	}
	for (i = 0; i<TreeNode.key_num; i++, j++)
	{
		memcpy(temp.character, BlockContent + UsingSize, TreeNode.key_size);

		TreeNode.KeySet.insert(TreeNode.KeySet.begin()+i, temp.integer);
		//TreeNode.KeySet.push_back(temp.integer);
		//
		UsingSize += TreeNode.key_size;
		memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
		TreeNode.OffsetSet.insert(TreeNode.OffsetSet.begin() + j, temp.integer);
		//TreeNode.OffsetSet.push_back(temp.integer);
		//TreeNode.OffsetSet[j] = temp.integer;
		UsingSize += sizeof(OffsetType);
	}

}



void ParseNode(char * BlockContent, Node<float> & TreeNode)
{
	data_trans temp;
	string temp_key;
	int i, j;
	int UsingSize = 0;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	TreeNode.key_num = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(bool));
	UsingSize += sizeof(bool);
	TreeNode.IsLeaf = temp.boolvar;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.Parent = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.Self = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.NextNode = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.PrevNode = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	TreeNode.key_size = temp.integer;

	j = 0;
	if (!TreeNode.IsLeaf)
	{
		memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
		//TreeNode.OffsetSet[j++] = temp.integer;
		TreeNode.OffsetSet.insert(TreeNode.OffsetSet.begin() + j, temp.integer);
		j++;
		//TreeNode.OffsetSet.push_back(temp.integer);
		UsingSize += sizeof(OffsetType);
	}
	for (i = 0; i<TreeNode.key_num; i++, j++)
	{
		memcpy(temp.character, BlockContent + UsingSize, TreeNode.key_size);
		TreeNode.KeySet.insert(TreeNode.KeySet.begin() + i, temp.decimal);
		//TreeNode.KeySet[i] = temp.decimal;
		UsingSize += TreeNode.key_size;
		memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
		TreeNode.OffsetSet.insert(TreeNode.OffsetSet.begin() + j, temp.integer);
		//TreeNode.OffsetSet.push_back(temp.integer);
		//TreeNode.OffsetSet[j] = temp.integer;
		UsingSize += sizeof(OffsetType);
	}
}

void ParseNode(char * BlockContent, Node<string> & TreeNode)
{
	data_trans temp;
	string temp_key;
	char s[256];
	int i, j;
	int UsingSize = 0;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	TreeNode.key_num = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(bool));
	UsingSize += sizeof(bool);
	TreeNode.IsLeaf = temp.boolvar;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.Parent = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.Self = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.NextNode = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);
	TreeNode.PrevNode = temp.integer;

	memcpy(temp.character, BlockContent + UsingSize, sizeof(int));
	UsingSize += sizeof(int);
	TreeNode.key_size = temp.integer;

	j = 0;
	if (!TreeNode.IsLeaf)
	{
		memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
		//TreeNode.OffsetSet.push_back(temp.integer);
		TreeNode.OffsetSet.insert(TreeNode.OffsetSet.begin() + j, temp.integer);
		//TreeNode.OffsetSet[j++] = temp.integer;
		j++;
		UsingSize += sizeof(OffsetType);
	}
	for (i = 0; i<TreeNode.key_num; i++, j++)
	{
		memcpy(s, BlockContent + UsingSize, TreeNode.key_size);
		temp_key = s;
		TreeNode.KeySet.insert(TreeNode.KeySet.begin() + i, temp_key);
		//TreeNode.KeySet[i] = temp_key;
		UsingSize += TreeNode.key_size;
		memcpy(temp.character, BlockContent + UsingSize, sizeof(OffsetType));
		TreeNode.OffsetSet.insert(TreeNode.OffsetSet.begin() + j, temp.integer);
		//TreeNode.OffsetSet.push_back(temp.integer);
		//TreeNode.OffsetSet[j] = temp.integer;
		UsingSize += sizeof(OffsetType);
	}
}


/*********************************************************************************************************************/
void Inver_ParseNode(char * BlockContent, Node<int> & TreeNode)
{
	data_trans temp;
	int i, j;
	int UsingSize = 0;

	temp.integer = TreeNode.key_num;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	temp.boolvar = TreeNode.IsLeaf;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(bool));
	UsingSize += sizeof(bool);

	temp.integer = TreeNode.Parent;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.Self;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.NextNode;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.PrevNode;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.key_size;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	j = 0;
	if (!TreeNode.IsLeaf)
	{
		temp.integer = TreeNode.OffsetSet[j++];
		memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
		UsingSize += sizeof(OffsetType);
	}
	for (i = 0; i<TreeNode.key_num; i++, j++)
	{
		temp.integer = TreeNode.KeySet[i];
		memcpy(BlockContent + UsingSize, temp.character, TreeNode.key_size);
		UsingSize += TreeNode.key_size;
		temp.integer = TreeNode.OffsetSet[j];
		memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
		UsingSize += sizeof(OffsetType);
	}
}


void Inver_ParseNode(char * BlockContent, Node<float> & TreeNode)
{
	data_trans temp;
	int i, j;
	int UsingSize = 0;

	temp.integer = TreeNode.key_num;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	temp.boolvar = TreeNode.IsLeaf;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(bool));
	UsingSize += sizeof(bool);

	temp.integer = TreeNode.Parent;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.Self;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.NextNode;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.PrevNode;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.key_size;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	j = 0;
	if (!TreeNode.IsLeaf)
	{
		temp.integer = TreeNode.OffsetSet[j++];
		memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
		UsingSize += sizeof(OffsetType);
	}
	for (i = 0; i<TreeNode.key_num; i++, j++)
	{
		temp.decimal = TreeNode.KeySet[i];
		memcpy(BlockContent + UsingSize, temp.character, TreeNode.key_size);
		UsingSize += TreeNode.key_size;
		temp.integer = TreeNode.OffsetSet[j];
		memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
		UsingSize += sizeof(OffsetType);
	}
}


void Inver_ParseNode(char * BlockContent, Node<string> & TreeNode)
{
	data_trans temp;
	char temp_key[256];
	int i, j;
	int UsingSize = 0;

	temp.integer = TreeNode.key_num;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(int));
	UsingSize += sizeof(int);

	temp.boolvar = TreeNode.IsLeaf;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(bool));
	UsingSize += sizeof(bool);

	temp.integer = TreeNode.Parent;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.Self;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.NextNode;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.PrevNode;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	temp.integer = TreeNode.key_size;
	memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
	UsingSize += sizeof(OffsetType);

	j = 0;
	if (!TreeNode.IsLeaf)
	{
		temp.integer = TreeNode.OffsetSet[j++];
		memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
		UsingSize += sizeof(OffsetType);
	}
	for (i = 0; i<TreeNode.key_num; i++, j++)
	{
		strcpy(temp_key, TreeNode.KeySet[i].c_str()); //若为string, 则需要注意一下！！！！！
		memcpy(BlockContent + UsingSize, temp_key, TreeNode.key_size);
		UsingSize += TreeNode.key_size;
		temp.integer = TreeNode.OffsetSet[j];
		memcpy(BlockContent + UsingSize, temp.character, sizeof(OffsetType));
		UsingSize += sizeof(OffsetType);
	}
}



