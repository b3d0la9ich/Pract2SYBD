#include "delete.h"

bool ExistColonk(const string& tableName, const string& columnName, Node* Tablehead) {
    if (!Tablehead) {
        cerr << "Ошибка: Список таблиц пуст.\n";
        return false;
    }

    Node* currentTable = Tablehead;  // Указатель на текущую таблицу в списке

    // Проходим по всем таблицам
    while (currentTable) {
        if (currentTable->table == tableName) {  // Если нашли нужную таблицу
            ListNode* currentColumn = currentTable->column;  // Указатель на первую колонку в таблице

            // Проходим по всем колонкам в найденной таблице
            while (currentColumn) {
                if (currentColumn->column_name == columnName) {  // Если нашли нужную колонку
                    return true;  // Колонка найдена
                }
                currentColumn = currentColumn->next;  // Переходим к следующей колонке
            }

            cerr << "Колонка " << columnName << " не найдена в таблице " << tableName << ".\n";
            return false;  // Колонка не найдена в этой таблице
        }

        currentTable = currentTable->next;  // Переходим к следующей таблице
    }

    cerr << "Таблица " << tableName << " не найдена.\n";
    return false;  // Таблица не найдена
}


// Функция для парсинга WHERE части команды
bool parseWhereClause(istringstream& iss2, string& table, string& column, string& value, const string& tableName, const TableJson& json_table) {
    string indication;
    iss2 >> indication;

    // Разделяем таблицу и колонку (table.column)
    if (indication.find('.') == string::npos) {
        cerr << "Некорректная команда.\n";
        return false;
    }

    size_t dotPos = indication.find('.');
    table = indication.substr(0, dotPos);
    column = indication.substr(dotPos + 1);

    if (table != tableName) {
        cerr << "Некорректная команда.\n";
        return false;
    }

    // Проверка на существование колонки
    if (!ExistColonk(tableName, column, json_table.Tablehead)) {
        cerr << "Такой колонки нет.\n";
        return false;
    }

    // Проверка знака "="
    iss2 >> indication;
    if (indication != "=") {
        cerr << "Некорректная команда.\n";
        return false;
    }

    // Проверка кавычек вокруг значения
    iss2 >> value;
    if (value.front() != '\'' || value.back() != '\'') {
        cerr << "Некорректная команда.\n";
        return false;
    }

    // Убираем кавычки из значения
    value = value.substr(1, value.size() - 2);
    return true;
}


bool deleteRowsFromTable(const string& tableName, const string& column, const string& value, const TableJson& json_table) {
    int amountCsv = 1;
    bool deletedStr = false;

    // Ищем все CSV файлы
    while (true) {
        fs::path filePath = fs::path("/home/b3d0la9a/don/Pract2SYBD") / json_table.Name / tableName / (to_string(amountCsv) + ".csv");
        ifstream file(filePath);
        if (!file.is_open()) {
            break;
        }
        file.close();
        amountCsv++;
    }

    // Просматриваем все CSV файлы
    for (size_t iCsv = 1; iCsv < amountCsv; iCsv++) {
        string filePath = "/home/b3d0la9a/don/Pract2SYBD/" + json_table.Name + "/" + tableName + "/" + (to_string(iCsv) + ".csv");
        rapidcsv::Document doc(filePath);

        int columnIndex = doc.GetColumnIdx(column);
        size_t amountRow = doc.GetRowCount();

        if (columnIndex == -1) {
            cerr << "Колонка не найдена в файле CSV: " << column << "\n";
            return false;
        }

        // Ищем и удаляем строки с нужным значением
        for (size_t i = 0; i < amountRow;) {  // Индекс не увеличивается сразу
            if (doc.GetCell<string>(columnIndex, i) == value) {
                doc.RemoveRow(i);
                deletedStr = true;
                amountRow--;  // Уменьшаем количество строк
                // Не увеличиваем индекс i, чтобы повторно проверить строку, которая переместилась на место удалённой
            } else {
                i++;  // Только увеличиваем индекс, если строка не удалена
            }
        }
        doc.Save(filePath);  // Сохраняем изменения в файл
    }

    return deletedStr;
}


bool delet(const string& command, const TableJson& json_table) {
    try {
        istringstream iss(command);
        string indication;

        // Проверка и разбор команды DELETE FROM
        if (!(iss >> indication && indication == "DELETE" && iss >> indication && indication == "FROM")) {
            cerr << "Некорректная команда.\n";
            return false;
        }

        string tableName;
        iss >> tableName;
        if (!TableExist(tableName, json_table.Tablehead)) {
            cerr << "Такой таблицы нет.\n";
            return false;
        }

        // Разбор второй части команды: WHERE <table.column> = '<value>'
        string whereCmd;
        if (!(iss >> whereCmd && whereCmd == "WHERE")) {
            cerr << "Некорректная команда.\n";
            return false;
        }

        string table, column, value;
        if (!parseWhereClause(iss, table, column, value, tableName, json_table)) {
            return false;  // Ошибка уже выведена в parseWhereClause
        }

        // Проверка на блокировку таблицы
        if (isloker(tableName, json_table.Name)) {
            cerr << "Таблица заблокирована.\n";
            return false;
        }
        loker(tableName, json_table.Name); // Блокировка таблицы

        // Попытка удалить строки из всех CSV файлов таблицы
        bool deletedStr = deleteRowsFromTable(tableName, column, value, json_table);

        // Разблокировка таблицы
        loker(tableName, json_table.Name);

        if (!deletedStr) {
            cout << "Указанное значение не найдено.\n";
            return false;
        }

        return true; // Успешное выполнение
    } catch (const std::exception& e) {
        cerr << "Ошибка выполнения DELETE: " << e.what() << "\n";
        return false; // В случае исключения возвращаем false
    }
}

