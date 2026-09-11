#include "DataModel.h"

DataModel::DataModel():DAQDataModelBase(){
	m_logger.SetServices(services);
	logger = &m_logger;
}

/*
TTree* DataModel::GetTTree(std::string name){

  return m_trees[name];

}


void DataModel::AddTTree(std::string name,TTree *tree){

  m_trees[name]=tree;

}


void DataModel::DeleteTTree(std::string name,TTree *tree){

  m_trees.erase(name);

}

*/

