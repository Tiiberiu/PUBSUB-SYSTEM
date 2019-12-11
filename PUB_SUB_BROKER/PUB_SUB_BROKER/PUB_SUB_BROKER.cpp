// PUB_SUB_BROKER.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "winsock2.h"
#include <ws2tcpip.h>
#include <iostream>

#include <array>
#include <list>
#include <map>
#include <mutex>
#include <random>
#include <conio.h>
#include <string>
#include <cstdlib>
#include <time.h>
#include <algorithm>
#include "json.hpp"

#pragma comment(lib, "Ws2_32.lib")

#define SYNC_PORT "12345" // the port client will be connecting to 
#define SYNC_HOSTNAME "localhost"
#define SYNC_TIME 2 // seconds
#define PORT_START "60"
#define SUBSIZE 512
#define PUBSIZE 256
#define BACKLOG 100
#define CONNECTION_CLOSED -2
#define INVALID_REQUEST -3
#define MAXDATASIZE 42
#define MAXLISTSIZE 1024
#define TEST_PORT "60881"
#define ever (;;)

using json = nlohmann::json;

struct subscriptie {

	std::string nume;
	std::string oper;
	std::string val;

};
// for convenience

std::mutex pubsMutex;
std::mutex brokersMutex;
std::mutex PUB_Mutex;

std::string BROKERID;

std::map < std::string, std::list<std::string>> subcriberList;

std::map < std::string, std::list<subscriptie>> subscriptionList;

std::map<std::string, std::string> activePubsList;
std::map<std::string, std::string> activeBrokersList;

std::list<std::string> publicationList;
std::list<std::string> connectedPublishers;


void WSAInit() {
	WSADATA wsaDATA;
	int err;
	if (0 != (err = WSAStartup(MAKEWORD(2, 2), &wsaDATA)))
		fprintf(stderr, "WSAStartup failed with Error: %d\n", err);
	else if (LOBYTE(wsaDATA.wVersion) != 2 || HIBYTE(wsaDATA.wVersion) != 2) {
		fprintf(stderr, "Could not find a usable version of Winsock.dll\n");
		err = -1;
	}
	//ERR POATE FI RETURNAT CA SA RAPORTEZE ERROAREA
}

void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}

	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

std::string generateID() {

	srand(time(NULL));
	int number = rand() % 900 + 100;
	return std::to_string(number);
};

std::string generateCON(std::string PUBID) {

	int i, j = 0;
	char buf[MAXDATASIZE];
	char part1[MAXDATASIZE] = "{\"TIP\":\"BRO\",\"ID\":\"";
	char part2[MAXDATASIZE] = "\",\"MSG\":\"CON\"}";
	for (i = 0; i < strlen(part1); i++)
		buf[j++] = part1[i];
	for (i = 0; i < strlen(PUBID.c_str()); i++)
		buf[j++] = PUBID[i];
	for (i = 0; i < strlen(part2); i++)
		buf[j++] = part2[i];
	buf[j] = '\0';
	return buf;
}

int checkSubscriptionOperator(std::string operat, std::string decompar, std::string compar) {

	if (operat == "=") {
		if (std::stoi(decompar) == std::stoi(compar)) {
			return 1;
		}
	}
	else if (operat == "<") {
		if (std::stoi(decompar) < std::stoi(compar)) {
			return 1;
		}
	}
	else if (operat == ">") {
		if (std::stoi(decompar) > std::stoi(compar)) {
			return 1;
		}
	}
	else if (operat == ">=") {
		if (std::stoi(decompar) >= std::stoi(compar)) {
			return 1;
		}
	}
	else if (operat == "<=") {
		if (std::stoi(decompar) <= std::stoi(compar)) {
			return 1;
		}
	}
	return 0;
}

void getPublicationFeed(int socket) {

	int data;
	char buffer[PUBSIZE];
	for ever{
		if ((data = recv(socket, buffer, PUBSIZE - 1, 0)) <= 0) {
			// got error or connection closed by client
			if (data == 0) {
				// connection closed
				printf("Publisher feed  hung up\n");
			}
			else {
				perror("Could not syncronize!\n");
				return;
			}
			printf("IDK recv error\n");
			return;
		}
		else {
			std::string str(buffer, std::find(buffer, buffer + PUBSIZE - 1, '\0'));
			if (!str.empty()) {
				PUB_Mutex.lock();
				publicationList.push_back(str);
				std::cout << str.c_str() << "\n";
				PUB_Mutex.unlock();
			}
		}
	}


}

void sendMachedPublication(int subscriber_sock, std::string pub) {

	int bytes;
	if ((bytes = send(subscriber_sock, pub.c_str(), PUBSIZE - 1, 0)) == -1) {
		perror("send");
		return;
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(50));

}

void filterSubscriptionFeed() {

	for ever{

		subscriptie temp_subscription;
		int result;
		std::list<std::string> temp_publicationList;
		std::map < std::string, std::list<subscriptie>> temp_subscriptionList;

		PUB_Mutex.lock();
			temp_publicationList = publicationList;
		PUB_Mutex.unlock();

		PUB_Mutex.lock();
			temp_subscriptionList = subscriptionList;
		PUB_Mutex.unlock();

		for (auto pub_it = temp_publicationList.begin(); pub_it != temp_publicationList.end(); ++pub_it) {
			// pentru fiecare publicatie cauta o subscriptie care sa se potriveasca
			json js = json::parse(*pub_it);
			std::string nota_film = js["nota_film"].get<std::string>();
			std::string nume_film = js["nume_film"].get<std::string>();
			std::string autor_film = js["autor_film"].get<std::string>();
			std::string an_film = js["an_film"].get<std::string>();
			std::string voturi_film = js["voturi_film"].get<std::string>();

			// pentru fiecare client-map 
			for (auto sub_it = temp_subscriptionList.begin(); sub_it != temp_subscriptionList.end(); ++sub_it) {
				std::string socket_subscriber = sub_it->first;
				std::list<subscriptie> info_subscriber = sub_it->second;

				// pentru fiecare subscriptie din map
				for (auto info_it = info_subscriber.begin(); info_it != info_subscriber.end(); ++info_it) {

					temp_subscription = (*info_it);
					if (temp_subscription.nume == "nota_film") {
						result = checkSubscriptionOperator(temp_subscription.oper, nota_film, temp_subscription.val);
						if (result) {
							sendMachedPublication(std::stoi(socket_subscriber), *pub_it);
						}
					}
					if (temp_subscription.nume == "an_film") {
						result = checkSubscriptionOperator(temp_subscription.oper, an_film, temp_subscription.val);
						if (result) {
							sendMachedPublication(std::stoi(socket_subscriber), *pub_it);
						}
					}
					if (temp_subscription.nume == "voturi_film") {
						result = checkSubscriptionOperator(temp_subscription.oper, voturi_film, temp_subscription.val);
						if (result) {
							sendMachedPublication(std::stoi(socket_subscriber), *pub_it);
						}
					}
					if (temp_subscription.nume == "autor_film") {
						if (autor_film == temp_subscription.val) {
							sendMachedPublication(std::stoi(socket_subscriber), *pub_it);
						}

					}
					if (temp_subscription.nume == "nume_film") {
						if (nume_film == temp_subscription.val) {
							sendMachedPublication(std::stoi(socket_subscriber), *pub_it);
						}

					}
				}

			}

		}
	}

}

int syncronizeCON(const char * ip, const char * port, std::string message,std::string &response = std::string(), int type = 0) {

	int sockfd, numbytes, rv;
	struct addrinfo hints, *servinfo, *p;
	char s[INET6_ADDRSTRLEN];
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if ((rv = getaddrinfo(ip, port, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and connect to the first we can
	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
			p->ai_protocol)) == -1) {
			perror("client: socket");
			continue;
		}

		if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			perror("client: connect");
			closesocket(sockfd);
			continue;
		}

		break;
	}

	if (p == NULL) {
		fprintf(stderr, "client: failed to connect\n");
		return 2;
	}
	if (type == 0) {
		inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
			s, sizeof s);
		printf("Broker: syncronizing with %s\n", s);

		freeaddrinfo(servinfo); // all done with this structure
		if ((numbytes = send(sockfd, message.c_str(), MAXDATASIZE - 1, 0)) == -1) {
			perror("send");
			exit(1);
		}

		printf("Syncronizing done...\n");
	}
	else if (type == 1){
		int data;
		char buffer[MAXLISTSIZE];
		freeaddrinfo(servinfo); // all done with this structure
		if ((numbytes = send(sockfd, message.c_str(), MAXDATASIZE - 1, 0)) == -1) {
			perror("send");
			exit(1);
		}
		if ((data = recv(sockfd, buffer, MAXLISTSIZE -1, 0)) <= 0) {
			// got error or connection closed by client
			if (data == 0) {
				// connection closed
				printf("Syncronizing server on socket %d hung up\n", sockfd);
			}
			else {
				perror("Could not syncronize!\n");
				return -1;
			}
			printf("IDK recv error\n");
			return -1;
		}
		response.assign(buffer, buffer + MAXLISTSIZE - 1);
	}
	else {


	}
	closesocket(sockfd);
	return 0;

}

std::map<std::string, std::string> selectAvailablePublisher() {


	std::map<std::string, std::string> con;
	if (activePubsList.empty()) {
		return con;
	}
	pubsMutex.lock();
	

	for (auto it = activePubsList.begin(); it != activePubsList.end(); it++)
	{
		std::string chk(it->first);
		if (std::find(connectedPublishers.begin(), connectedPublishers.end(), chk) != connectedPublishers.end())
		{

		}
		else {

			std::string pub_ip(activePubsList.begin()->second);
			std::string pub_port(activePubsList.begin()->first);
			connectedPublishers.push_back(pub_port);

			con[pub_port] = pub_ip;
			break;
		}

	}
	pubsMutex.unlock();

	return con;


}

int connectToPublisher() {

	std::map<std::string, std::string> pu;
	do {
		pu = selectAvailablePublisher();
		std::this_thread::sleep_for(std::chrono::seconds(SYNC_TIME));
	} while (pu.empty());
	std::string ip = pu.begin()->second;
	std::string port = "60" + pu.begin()->first;
	std::cout << port.c_str() << "\n";

	if (ip == "::1") {
		ip.clear();
		ip.assign("localhost");
	}
	port.assign(TEST_PORT);
	int sockfd, numbytes, rv;
	struct addrinfo hints, *servinfo, *p;
	char s[INET6_ADDRSTRLEN];
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if ((rv = getaddrinfo(ip.c_str(), port.c_str(), &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and connect to the first we can
	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
			p->ai_protocol)) == -1) {
			perror("client: socket");
			continue;
		}

		if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			perror("client: connect");
			closesocket(sockfd);
			continue;
		}

		break;
	}

	if (p == NULL) {
		fprintf(stderr, "failed to connect to pub\n");
		return 2;
	}
	return sockfd;
}

std::map<std::string, std::string> getPubList() {

	std::string CON_MSG = "{\"TIP\":\"BRO\",\"ID\":\"" + BROKERID + "\",\"MSG\":\"LSP\"}";
	std::string message;
	std::map<std::string, std::string> PUBS_INFO;
	if (syncronizeCON(SYNC_HOSTNAME, SYNC_PORT, CON_MSG, message, 1) != 0) {
		return{};
	}
	json js = json::parse(message);

	for (json::iterator it = js.begin(); it != js.end(); ++it) {
		//std::cout << *it << '\n';
		if (!(*it).empty()) {
			for (json::iterator it2 = (*it).begin(); it2 != (*it).end(); ++it2) {
				auto ip = (*it2)["IP"].get<std::string>();
				auto port = (*it2)["PORT"].get<std::string>();
				PUBS_INFO[port] = ip;
			}
		}
	}
	if (PUBS_INFO.empty())
		return{};
	return PUBS_INFO;
	//auto tip = js["TIP"].get<std::string>();
	//auto msg = js["MSG"].get<std::string>();
	//auto id = js["ID"].get<std::string>();

}

std::map<std::string, std::string> getBrokerList() {
	
	std::string CON_MSG = "{\"TIP\":\"BRO\",\"ID\":\"" + BROKERID + "\",\"MSG\":\"LSB\"}";
	std::string message;
	std::map<std::string, std::string> BROKER_INFO;
	if (syncronizeCON(SYNC_HOSTNAME, SYNC_PORT, CON_MSG, message, 1) != 0) {
		return{};
	}
	json js = json::parse(message);

	for (json::iterator it = js.begin(); it != js.end(); ++it) {
		//std::cout << *it << '\n';
		if (!(*it).empty()) {
			for (json::iterator it2 = (*it).begin(); it2 != (*it).end(); ++it2) {
				auto ip = (*it2)["IP"].get<std::string>();
				auto port = (*it2)["PORT"].get<std::string>();
				BROKER_INFO[port] = ip;
			}
		}
	}
	if (BROKER_INFO.empty())
		return{};
	return BROKER_INFO;

}

void syncronizeActivePubs() {

	for ever{

		std::map<std::string, std::string> tempList = getPubList();

		pubsMutex.lock();

		for (auto it = tempList.begin(); it != tempList.end(); ++it)
			activePubsList.insert(std::pair<std::string, std::string>(it->first, it->second));

		pubsMutex.unlock();
		std::this_thread::sleep_for(std::chrono::seconds(SYNC_TIME));
	}

}

void syncronizeActiveBrokers() {

	for ever{

		std::map<std::string, std::string> tempList = getBrokerList();

		brokersMutex.lock();

		for (auto it = tempList.begin(); it != tempList.end(); ++it)
			activeBrokersList.insert(std::pair<std::string, std::string>(it->first, it->second));

		brokersMutex.unlock();
		std::this_thread::sleep_for(std::chrono::seconds(SYNC_TIME));
	}

}

int handleNewConnection(int listener) {

	// handle new connections
	// handle new connections
	char remoteIP[INET6_ADDRSTRLEN];
	struct sockaddr_storage remoteaddr;
	socklen_t addrlen = sizeof remoteaddr;
	int newfd = accept(listener, (struct sockaddr *)&remoteaddr, &addrlen);

	if (newfd == -1) {
		perror("accept");
		return -1;
	}
	else {
		printf("Pub: new connection from %s on "
			"socket %d\n",
			inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr*)&remoteaddr), remoteIP, INET6_ADDRSTRLEN),
			newfd);
	}

	return newfd;
}

int handleNewData(int client) {

	// handle new connections
	int verif = 0;
	char remoteIP[INET6_ADDRSTRLEN];
	struct sockaddr_storage remoteaddr;
	socklen_t addrlen = sizeof remoteaddr;
	char buffer[SUBSIZE];    // buffer for client data
	int data;

	if ((data = recv(client, buffer, SUBSIZE - 1, 0)) <= 0) {
		// got error or connection closed by client
		if (data == 0) {
			// connection closed
			printf("selectserver: socket %d hung up\n", client);

		}
		else {
			perror("recv");
			return CONNECTION_CLOSED;
		}
		return CONNECTION_CLOSED;
	}
	else {
		std::string str(buffer, buffer + SUBSIZE - 1);
		subscriptie sub;
		json js = json::parse(str);
		auto nume_film = js["nume_film"]["operator"].get<std::string>();
		if (!nume_film.empty()) {

			sub.nume = "nume_film";
			sub.oper = js["nume_film"]["operator"].get<std::string>();
			sub.val = js["nume_film"]["valoare"].get<std::string>();
			verif = 1;

		} // pushback client, an-film, operator + valuare
		auto autor_film = js["autor_film"]["operator"].get<std::string>();
		if (!verif && !autor_film.empty() ) {

			sub.nume = "autor-film";
			sub.oper = js["autor_film"]["operator"].get<std::string>();
			sub.val = js["autor_film"]["valoare"].get<std::string>();
			verif = 1;

		}
		auto an_film = js["an_film"]["operator"].get<std::string>();
		if (!verif && !an_film.empty()) {

			sub.nume = "an_film";
			sub.oper = js["an_film"]["operator"].get<std::string>();
			sub.val = js["an_film"]["valoare"].get<std::string>();
			verif = 1;
		}
		auto nota_film = js["nota_film"]["operator"].get<std::string>();
		if (!verif && !nota_film.empty()) {

			sub.nume = "nota_film";
			sub.oper = js["nota_film"]["operator"].get<std::string>();
			sub.val = js["nota_film"]["valoare"].get<std::string>();
			verif = 1;
		}
		auto voturi_film = js["voturi_film"]["operator"].get<std::string>();
		if (!verif && !voturi_film.empty()) {

			sub.nume = "voturi_film";
			sub.oper = js["voturi_film"]["operator"].get<std::string>();
			sub.val = js["voturi_film"]["valoare"].get<std::string>();
			verif = 1;
		}

		if (verif) {
			std::string client_string_sock(std::to_string(client));

			brokersMutex.lock();
				subscriptionList[client_string_sock].push_back(sub);
			brokersMutex.unlock();
		}
		else {

			std::cout << "Not recognized json received: " << str.c_str()<<'\n';
		}
		//std::cout << str << '\n';

		//std::cout << tip << " " << id << " " << msg << " ";

	}
	return 1;
}

struct addrinfo* SetupAddress(const char *port) {


	struct addrinfo hints, p, *rez;

	memset(&hints, 0, sizeof hints);

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	printf("Setting up server address\n");

	int addrError = getaddrinfo(NULL, port, &hints, &rez);
	if (addrError != 0) {
		fprintf(stderr, "getaddrinfo: %d\n", WSAGetLastError());
		exit(-1);
	}
	return rez;
}

int Bind(const char *port) {

	int socket_server;
	const char yes = 1;
	addrinfo *p, *rez = SetupAddress(port);
	for (p = rez; p != NULL; p = p->ai_next) {
		if ((socket_server = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
			fprintf(stderr, "server socket: %d\n", WSAGetLastError());
			continue;
		}

		if (setsockopt(socket_server, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
			fprintf(stderr, "setsockopt: %d\n", WSAGetLastError());
			exit(1);
		}

		if (bind(socket_server, p->ai_addr, p->ai_addrlen) == -1) {
			closesocket(socket_server);
			fprintf(stderr, "server bind: %d\n", WSAGetLastError());
			continue;
		}

		char str[INET_ADDRSTRLEN];

		inet_ntop(AF_INET, &((struct sockaddr_in *)p->ai_addr)->sin_addr, str, INET_ADDRSTRLEN);

		printf("Adresa locala este: %s \n", str);

		break;
	}

	freeaddrinfo(p); // eliberam structura addrinfo
	if (p == NULL) {
		fprintf(stderr, "server: failed to bind\n");
		exit(1);
	}

	return socket_server;
}

void Listener(const char *port) {

	fd_set master;    // master file descriptor list
	fd_set read_fds;  // temp file descriptor list for select()
	int fdmax, i, j;        // maximum file descriptor number

	int listener;     // listening socket descriptor
	int newfd;        // newly accept()ed socket descriptor
	char buf[256];    // buffer for client data
	int nbytes;

	char remoteIP[INET6_ADDRSTRLEN];
	struct sockaddr_storage remoteaddr; // client address
	socklen_t addrlen;

	FD_ZERO(&master);
	FD_ZERO(&read_fds);

	int socket_server = Bind(port);


	if (listen(socket_server, BACKLOG) == SOCKET_ERROR) {
		fprintf(stderr, "Failed to listen for clients: %d\n", WSAGetLastError());
		exit(-1);
	}

	FD_SET(socket_server, &master);

	fdmax = socket_server;

	for (;;) {
		read_fds = master; // copy it
		if (select(NULL, &read_fds, NULL, NULL, NULL) == -1) {
			perror("select");
			exit(4);
		}
		for (i = 0; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // we got one!!
				if (i == socket_server) { // new client
					newfd = handleNewConnection(i);
					if (newfd != -1) {
						FD_SET(newfd, &master); // add to master set
						if (newfd > fdmax)  // keep track of the max
							fdmax = newfd;
					}
				}
				else {
					if (handleNewData(i) == CONNECTION_CLOSED) {
				
						closesocket(i); // bye!
						FD_CLR(i, &master); // remove from master set
					}

				} // END handle data from client
			} // END got new incoming connection
		} // END looping through file descriptors
	}


}

int main()
{
	BROKERID = generateID();
	std::string CON_MSG = generateCON(BROKERID);
	std::cout << BROKERID << '\n';

	WSAInit();

	std::thread t4(filterSubscriptionFeed); t4.detach();
	//std::this_thread::sleep_for(std::chrono::seconds(50));

	//ADD PUB TO THE LIST OF PUBS
	if (syncronizeCON(SYNC_HOSTNAME, SYNC_PORT, CON_MSG) != 0) {

		return -1;
	}
	std::thread t1(syncronizeActivePubs); t1.detach();
	std::thread t2(syncronizeActiveBrokers); t2.detach();
	
	//Select publisher to get feed from

	int pub_sock = connectToPublisher();
	std::thread t3(getPublicationFeed, pub_sock); t3.detach();

	//Start listening for subscriptions

	std::string pub_port = PORT_START + BROKERID;
	Listener(pub_port.c_str());
    return 0;
}

