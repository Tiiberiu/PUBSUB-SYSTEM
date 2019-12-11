// PUB_SUB_Syncronizer.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "stdafx.h"

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

#include "json.hpp"

#include "winsock2.h"
#include <ws2tcpip.h>

#pragma comment(lib, "Ws2_32.lib")

using json = nlohmann::json;

#define msg_len 36
#define BACKLOG 10     // how many pending connections queue will hold
#define PORT "12345"   // the port users will be connecting to
#define CONNECTION_CLOSED -2
#define INVALID_REQUEST -3

#define PUB "PUB"
#define BRO "BRO"
#define CON "CON"
#define LSB "LSB"
#define LSP "LSP"

struct addrinfo  *server_ip_struct = nullptr;
struct broker_info {

	std::string ip;
	std::string id;

}_info[10000];

std::vector<std::string> brokers;
std::vector<std::string> publishers;
std::vector < int > connected_brokers;
std::vector < int > connected_pubs;


//TIP : "PUB", "ID":"100", MSG : "CON"
//TIP : "BRO", MSG : "CON"
//TIP : "BRO", MSG : "LST"
//TIP : "BRO", MSG : "CON"

void WSAInit() { //INITIALIZARE WINSOCK DOAR PENTRU WINDOWS
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

void* get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

// somthing[id] = socket
std::string getSockIp(int sock) {

	return _info[sock].ip;
}
std::string getSockId(int sock) {

	return _info[sock].id;
}

int handleNewConnection(int listener) {

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

		_info[newfd].ip = inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr*)&remoteaddr), remoteIP, INET6_ADDRSTRLEN);
		printf("selectserver: new connection from %s on "
			"socket %d\n",
			inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr*)&remoteaddr), remoteIP, INET6_ADDRSTRLEN),
			newfd);
	}

	return newfd;
}

void filterConnectedPubs() {


}

int handleNewData(int client) {

	// handle new connections
	char remoteIP[INET6_ADDRSTRLEN];
	struct sockaddr_storage remoteaddr;
	socklen_t addrlen = sizeof remoteaddr;
	char buffer[42];    // buffer for client data
	int data;

	if ((data = recv(client, buffer, sizeof buffer, 0)) <= 0) {
		// got error or connection closed by client
		if (data == 0) {
			// connection closed
			printf("selectserver: socket %d hung up\n", client);

			//connected_brokers.erase(std::remove(connected_brokers.begin(), connected_brokers.end(), client), connected_brokers.end());
			//connected_pubs.erase(std::remove(connected_pubs.begin(), connected_pubs.end(), client), connected_pubs.end());

		}
		else {
			perror("recv");
			return CONNECTION_CLOSED;
		}
		return CONNECTION_CLOSED;
	}
	else {
		std::string str(buffer, buffer + msg_len);
		//std::cout << str << '\n';
		json js = json::parse(str);
		auto tip = js["TIP"].get<std::string>();
		auto msg = js["MSG"].get<std::string>();
		auto id = js["ID"].get<std::string>();

		std::cout << tip << " " << id << " " << msg << " ";

		if (tip == BRO) {
			if (msg == CON) {
				if (std::find(brokers.begin(), brokers.end(), id) != brokers.end())
				{

					brokers.erase(std::remove(brokers.begin(), brokers.end(), id), brokers.end());
					_info[client].id = id;
					brokers.push_back(id);

				}
				else {
					connected_brokers.push_back(client);
					_info[client].id = id;
					brokers.push_back(id);

				}
			}
			else if (msg == LSB) {
				// {"IPS":["192.168.1.102","192.168.1.103"]}
				//  {"IPS":[{"IP":"192.168.0.1", "PORT":"60000"}]};
				int cnt = 1;
				std::vector<std::string> broker_ips;
				std::vector<std::string> broker_ports;
				char buf[1024];
				std::string ips_msg = "{\"IPS\":[";
				for (auto it = connected_brokers.begin(); it != connected_brokers.end(); ++it) {
					if (*it != client) {
						broker_ips.push_back(getSockIp(*it));
						broker_ports.push_back(getSockId(*it));
					}
				}
				while (!broker_ips.empty())
				{
					//{"IP":"192.168.0.1", "PORT":"60000"}
					if (broker_ips.size() == 1) {

						ips_msg = ips_msg + "{";
						ips_msg = ips_msg + "\"IP\":\"" + broker_ips.back() + "\",";
						ips_msg = ips_msg + "\"PORT\":\"" + broker_ports.back() + "\"";
						ips_msg = ips_msg + "}";
					}
					else {

						ips_msg = ips_msg + "{";
						ips_msg = ips_msg + "\"IP\":\"" + broker_ips.back() + "\",";
						ips_msg = ips_msg + "\"PORT\":\"" + broker_ports.back() + "\"";
						ips_msg = ips_msg + "},";
					}


					broker_ips.pop_back();
					broker_ports.pop_back();
				}
				ips_msg += "]}";
				//strcpy_s(buf, ips_msg.length(), ips_msg.c_str());
				if ((data = send(client, ips_msg.c_str(), 1023, 0)) == -1) {
					perror("send");
					exit(1);
				}
			}
			else if (msg == LSP) {
				// {"IP1":"192.168.1.102,"IP2":192.168.1.103"}

				int cnt = 1;
				std::vector<std::string> pubs_ips;
				std::vector<std::string> pubs_ports;
				char buf[1024];
				std::string ips_msg = "{\"IPS\":[";
				for (auto it = connected_pubs.begin(); it != connected_pubs.end(); ++it) {
					if (*it != client) {
						pubs_ips.push_back(getSockIp(*it));
						pubs_ports.push_back(getSockId(*it));
					}
				}
				while (!pubs_ips.empty())
				{
					//{"IP":"192.168.0.1", "PORT":"60000"}
					if (pubs_ips.size() == 1) {
						ips_msg = ips_msg + "{";
						ips_msg = ips_msg + "\"IP\":\"" + pubs_ips.back() + "\",";
						ips_msg = ips_msg + "\"PORT\":\"" + pubs_ports.back() + "\"";
						ips_msg = ips_msg + "}";
					}
					else {
						ips_msg = ips_msg + "{";
						ips_msg = ips_msg + "\"IP\":\"" + pubs_ips.back() + "\",";
						ips_msg = ips_msg + "\"PORT\":\"" + pubs_ports.back() + "\"";
						ips_msg = ips_msg + "},";
					}
					pubs_ports.pop_back();
					pubs_ips.pop_back();
				}
				ips_msg += "]}";
				//strcpy_s(buf, ips_msg.length(), ips_msg.c_str());
				if ((data = send(client, ips_msg.c_str(), 1023, 0)) == -1) {
					perror("send");
					exit(1);
				}
			}
			else return INVALID_REQUEST;

		}
		else if (tip == PUB) {
			if (msg == CON) {
				if (std::find(publishers.begin(), publishers.end(), id) != publishers.end())
				{
					publishers.erase(std::remove(publishers.begin(), publishers.end(), id), publishers.end());
					publishers.push_back(id);
					_info[client].id = id;
				}
				else {
					connected_pubs.push_back(client);
					publishers.push_back(id);
					_info[client].id = id;
				}

			}
			else return INVALID_REQUEST;

		}
		else return INVALID_REQUEST;


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

	server_ip_struct = p;
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
					//	connected_brokers.erase(std::remove(connected_brokers.begin(), connected_brokers.end(), i), connected_brokers.end());
					//	connected_pubs.erase(std::remove(connected_pubs.begin(), connected_pubs.end(), i), connected_pubs.end());

						closesocket(i); // bye!
						FD_CLR(i, &master); // remove from master set
					}

				} // END handle data from client
			} // END got new incoming connection
		} // END looping through file descriptors
	}


}

int main() {

	WSAInit();
	Listener(PORT);
}