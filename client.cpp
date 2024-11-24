#include <iostream>
#include <sys/socket.h> // функции для работы с сокетами
#include <netinet/in.h> // структуры данных для портов
#include <arpa/inet.h> // функции для работы с сетевыми адресами
#include <unistd.h> // функции для работы с системными вызовами
#include <string.h>

using namespace std;

int main() {

    // Создание клиентского сокета
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0); 
    if (clientSocket == -1) {
        cerr << "Не удалось создать сокет\n";
        return 1;
    }

    // определение адреса сервера
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(7432);

    // Преобразование IP адреса
    if (inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr) <= 0) { // преобразует текстовое представление IP-адреса в двоичную форму
        cerr << "Неправильный адрес\n";
        close(clientSocket);
        return 1;
    }
    
    // соединение с сервером
    if (connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        cerr << "Соединение не принято\n";
        close(clientSocket);
        return 1;
    }

    char buffer[1024] = {0};
    while (true) {
        cout << "Сообщение для отправки: ";
        string text;
        getline(cin, text);
        text += "\n";
        send(clientSocket, text.c_str(), text.length(), 0); // отправляет сообщение на сервер, c_str - указатель на строку
        int valread = read(clientSocket, buffer, 1024); // чтение данных в буфер, valread - количество байт
        if (valread <= 0) {
            cerr << "Сервер отсоединился\n";
            break;
        }

        cout << "Сообщение с сервера: " << buffer;
        memset(buffer, 0, sizeof(buffer)); // очищаем и заполняем буфер нулями
    }
    close(clientSocket);
    return 0;
}