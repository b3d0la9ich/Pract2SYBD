#include <iostream>
#include <string>
#include <thread> // для многопоточности
#include <mutex> // для синхронизации потоков
#include <netinet/in.h> // 
                        // для работы с сокетами
#include <unistd.h>     //
#include "parcer.h"
#include "insert.h"
#include "delete.h"
#include "select.h"

std::mutex db_mutex; // Нужен для синхронизации между потоками

// Функция обработки клиента
void handle_client(int client_socket, TableJson& json_table) {
    char buffer[1024] = {0}; // Буфер для приема данных от клиента
    while (true) {
        int valread = read(client_socket, buffer, 1024); // Читает данные от клиента через сокет и записывает в буфер
        if (valread <= 0) {
            std::cerr << "Клиент отключился\n";
            break;
        }

        // Обработка команды
        std::string command(buffer);
        command.erase(command.find_last_not_of("\n\r") + 1); // Убираем символы новой строки (чтобы команда была чистой)
        std::cout << "Получено: " << command << std::endl;

        // Выбор действия
        std::string response;
        if (command.find("INSERT") == 0) {
            std::lock_guard<std::mutex> lock(db_mutex); // захватывает мьютекс на время работы с базой данных (только один поток работает)
            insert(command, json_table);
            response = "INSERT выполнено успешно\n";
        } else if (command.find("DELETE") == 0) {
            std::lock_guard<std::mutex> lock(db_mutex);
            delet(command, json_table);
            response = "DELETE выполнено успешно\n";
        } else if (command.find("SELECT") == 0) {
            std::lock_guard<std::mutex> lock(db_mutex);
            select(command, json_table);
            response = "SELECT выполнено успешно\n";
        } else {
            response = "Неверная команда\n";
        }

        // Отправка ответа клиенту
        send(client_socket, response.c_str(), response.size(), 0);
        memset(buffer, 0, sizeof(buffer)); // Очищаем буфер
    }
    close(client_socket); // завершение работы с клиентом
}

int main() {
    TableJson json_table;
    parser(json_table);

    int server_fd; // файловый дескриптор сокета, служит для подключения клиентов
    int new_socket; // дескриптор сокета для конкретного подключения клиента
    struct sockaddr_in address; // структура, описывающая адрес сервера
    int opt = 1; // опция для настройки сокета
    int addrlen = sizeof(address); // размер структуры address

    // Создаем серверный сокет
    server_fd = socket(AF_INET, SOCK_STREAM, 0); // Используется IPv4 / Сокет работает через TCP / Протокол для TCP
    if (server_fd == 0) {
        perror("Ошибка создания сокета");
        exit(EXIT_FAILURE);
    }

    // Настраиваем сокет
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) { // сервер может повторно использовать адрес 
                                                                                             // и порт после перезапуска
        perror("Ошибка настройки сокета");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;           // Указывает, что используется IPv4
    address.sin_addr.s_addr = INADDR_ANY;   // Устанавливает адрес, на который сервер будет принимать подключения (в данном случае локальный адрес)
    address.sin_port = htons(7432);         // Устанавливает порт для подключения клиентов (7432) 

    // Привязываем сокет к адресу
    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("Ошибка привязки");
        exit(EXIT_FAILURE);
    }

    // Запускаем прослушивание
    if (listen(server_fd, 3) < 0) {
        perror("Ошибка прослушивания");
        exit(EXIT_FAILURE);
    }

    std::cout << "Сервер запущен. Ожидание подключения клиентов...\n";

    while (true) {
        new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);   // блокирует выполнение до того пока клиент не подключится
                                                                                            // создает новый сокет для взаимодействия с подключившимся клиентом
        if (new_socket < 0) {
            perror("Ошибка подключения клиента");
            continue;
        }
        std::cout << "Клиент подключился\n";

        // Создаем отдельный поток для обработки клиента
        std::thread(handle_client, new_socket, std::ref(json_table)).detach(); // detach делает поток независимым - позволяет серверу продолжить принимать других клиентов
    }

    return 0;
}
