#pragma once
#include <iostream>
#include <filesystem>
#include "rapidcsv.h" 
#include "Node.h"

using namespace std;
namespace fs = filesystem;

bool TableExist(const string& tableName, Node* tableHead);
bool isloker(const string& tableName, const string& schemeName);
void copyNameColonk(const string& from_file, const string& to_file);
void loker(const string& tableName, const string& schemeName);
int findCsvFileCount(const TableJson& json_table, const string& tableName);
void createNewCsvFile(const std::string& baseDir, const std::string& tableName, int& csvNumber, const TableJson& tableJson);
bool insert(const string& command, TableJson& json_table);