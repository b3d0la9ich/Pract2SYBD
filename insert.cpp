#include "insert.h"

bool TableExist(const string& tableName, Node* tableHead) {
    Node* current = tableHead;
    while (current) {
        if (current->table == tableName) {
            return true;
        }
        current = current->next;
    }
    return false;
}

bool isloker(const string& tableName, const string& schemeName) {
    string baseDir = "/home/b3d0la9a/don/Pract2SYBD/" + schemeName + "/" + tableName;
    string lockFile = baseDir + "/" + (tableName + "_lock.txt");

    if (!fs::exists(lockFile)) {
        cerr << "Ошибка: файл блокировки не существует: " << lockFile << ".\n";
        return false;
    }

    ifstream file(lockFile);
    if (!file.is_open()) {
        cerr << "Ошибка: не удалось открыть файл блокировки: " << lockFile << ".\n";
        return false;
    }

    string current;
    file >> current;
    file.close();
    return current == "locked";
}

void loker(const string& tableName, const string& schemeName) {
    string baseDir = "/home/b3d0la9a/don/Pract2SYBD/" + schemeName + "/" + tableName;
    string lockFile = baseDir + "/" + (tableName + "_lock.txt");

    if (!fs::exists(lockFile)) {
        cerr << "Ошибка: файл блокировки не существует: " << lockFile << "\n";
        return;
    }

    ifstream fileIn(lockFile);
    if (!fileIn.is_open()) {
        cerr << "Не удалось открыть файл блокировки: " << lockFile << "\n";
        return;
    }

    string current;
    fileIn >> current;
    fileIn.close();

    ofstream fileOut(lockFile);
    if (!fileOut.is_open()) {
        cerr << "Не удалось открыть файл для записи блокировки: " << lockFile << "\n";
        return;
    }

    fileOut << (current == "locked" ? "unlocked" : "locked");
    fileOut.close();
}

// Функция для копирования названий колонок из одного файла в другой
void copyNameColonk(const string& from_file, const string& to_file) {
    string columns;
    ifstream fileF(from_file); // открываем файл для чтения колонок
    if (!fileF.is_open()) {
        cerr << "Не удалось открыть файл: " << from_file << "\n";
        return;
    }
    fileF >> columns;
    fileF.close();

    ofstream fileT(to_file); // открываем файл для записи колонок
    if (!fileT.is_open()) {
        cerr << "Не удалось открыть файл: " << to_file << "\n";
        return;
    }
    fileT << columns << endl;
    fileT.close();
}

int findCsvFileCount(const TableJson& json_table, const string& tableName) {
    int csvCount = 0;
    int csvNumber = 1;

    while (true) {
        string csvFile = "/home/b3d0la9a/don/Pract2SYBD/" + json_table.Name + "/" + tableName + "/" + (to_string(csvNumber) + ".csv");

        // Проверяем, существует ли файл
        ifstream fileIn(csvFile);
        if (!fileIn.is_open()) {
            // Файл не существует, выходим из цикла, так как дальше файлов нет
            break;
        }
        fileIn.close();  // Закрываем файл после проверки

        // Увеличиваем счётчик найденных файлов
        csvCount++;
        
        // Переходим к следующему файлу
        csvNumber++;
    }

    // Возвращаем общее количество существующих файлов
    return csvCount;
}

void createNewCsvFile(const string& baseDir, const string& tableName, int& csvNumber, const TableJson& tableJson) {
    // Получаем максимальное количество строк на файл из структуры TableJson
    int maxRowsPerFile = tableJson.TableSize;
    
    // Формируем путь к текущему CSV файлу
    string csvFile = baseDir + "/" + tableName + "/" + to_string(csvNumber) + ".csv";
    
    // Проверяем количество строк в текущем файле
    rapidcsv::Document doc(csvFile);
    if (doc.GetRowCount() >= maxRowsPerFile) {
        // Если достигнут лимит строк, увеличиваем номер файла
        csvNumber++;
        csvFile = baseDir + "/" + tableName + "/" + to_string(csvNumber) + ".csv";
    }

    // Если файла нет, создаём его
    if (!fs::exists(csvFile)) {
        // Создаём новый файл и копируем в него названия колонок
        string csvFirst = baseDir + "/" + tableName + "/1.csv";
        copyNameColonk(csvFirst, csvFile);
    }
}

bool insert(const string& command, TableJson& json_table) {
    try {
        istringstream iss(command);
        string slovo;

        // Проверяем ключевые слова "INSERT INTO"
        iss >> slovo >> slovo;
        if (slovo != "INTO") {
            cerr << "Некорректная команда: отсутствует 'INTO'.\n";
            return false;
        }

        // Читаем имя таблицы
        string tableName;
        iss >> tableName;
        if (!TableExist(tableName, json_table.Tablehead)) {
            cerr << "Ошибка: таблица '" << tableName << "' не существует.\n";
            return false;
        }

        // Проверяем ключевое слово "VALUES"
        iss >> slovo;
        if (slovo != "VALUES") {
            cerr << "Некорректная команда: отсутствует 'VALUES'.\n";
            return false;
        }

        // Считываем значения
        string values;
        while (iss >> slovo) {
            values += slovo;
        }
        if (values.empty() || values.front() != '(' || values.back() != ')') {
            cerr << "Ошибка: значения должны быть в формате (…).\n";
            return false;
        }

        // Проверяем, заблокирована ли таблица
        if (isloker(tableName, json_table.Name)) {
            cerr << "Ошибка: таблица '" << tableName << "' заблокирована.\n";
            return false;
        }

        // Устанавливаем блокировку
        loker(tableName, json_table.Name);

        // Работа с Primary Key
        int currentPK;
        string PKFile = "/home/b3d0la9a/don/Pract2SYBD/" + json_table.Name + "/" + tableName + "/" + (tableName + "_pk_sequence.txt");

        ifstream fileIn(PKFile);
        if (!fileIn.is_open()) {
            cerr << "Ошибка: не удалось открыть файл PK.\n";
            loker(tableName, json_table.Name); // Снимаем блокировку
            return false;
        }

        fileIn >> currentPK;
        fileIn.close();

        ofstream fileOut(PKFile);
        if (!fileOut.is_open()) {
            cerr << "Ошибка: не удалось записать в файл PK.\n";
            loker(tableName, json_table.Name); // Снимаем блокировку
            return false;
        }

        currentPK++;
        fileOut << currentPK;
        fileOut.close();

        // Определяем номер текущего CSV файла
        int csvNumber = findCsvFileCount(json_table, tableName);

        // Путь к директории таблицы
        string baseDir = "/home/b3d0la9a/don/Pract2SYBD/" + json_table.Name;

        // Создаём новый CSV файл при необходимости
        createNewCsvFile(baseDir, tableName, csvNumber, json_table);

        // Путь к текущему CSV файлу
        string csvSecond = baseDir + "/" + tableName + "/" + to_string(csvNumber) + ".csv";

        // Открываем CSV файл для записи
        ofstream csv(csvSecond, ios::app);
        if (!csv.is_open()) {
            cerr << "Ошибка: не удалось открыть CSV файл.\n";
            loker(tableName, json_table.Name); // Снимаем блокировку
            return false;
        }

        // Записываем данные в CSV файл
        csv << currentPK << ",";
        for (size_t i = 0; i < values.size(); ++i) {
            if (values[i] == '\'') {
                ++i;
                while (i < values.size() && values[i] != '\'') {
                    csv << values[i++];
                }
                if (i + 1 < values.size() && values[i + 1] != ')') {
                    csv << ",";
                } else {
                    csv << endl;
                }
            }
        }

        csv.close();
        loker(tableName, json_table.Name); // Снимаем блокировку
        return true; // Успешное выполнение
    } catch (const std::exception& e) {
        cerr << "Ошибка выполнения INSERT: " << e.what() << "\n";
        return false; // В случае исключения возвращаем false
    }
}

