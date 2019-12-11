// PUB_SUB_Publisher.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "winsock2.h"
#include <ws2tcpip.h>
#include <iostream>
#include <array>
#include <list>
#include <mutex>
#include <random>
#include <conio.h>
#include <string>
#include <cstdlib>
#include <time.h>

#pragma comment(lib, "Ws2_32.lib")

#define SYNC_PORT "12345" // the port client will be connecting to 
#define SYNC_HOSTNAME "localhost"
#define PORT_START "60"
#define MAXDATASIZE 42
#define MSGRATE 50 // 50 messages per second
#define BACKLOG 100
#define MAX_PUB 10
#define AN_MAX 2019
#define AN_MIN 1940
#define VOTURI_MAX 10000
#define PUBSIZE 256

// dont touch this
std::list<std::string> publicationList;
std::string PUBID;
unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();

std::mutex PUB_Mutex;
std::string nume_autori[101] = { "THOMAS","JAMES","JACK","DANIEL","MATTHEW","RYAN","JOSHUA","LUKE","SAMUEL","JORDAN","ADAM","MICHAEL","ALEXANDER","CHRISTOPHER","BENJAMIN","JOSEPH","LIAM","JAKE","WILLIAM","ANDREW","GEORGE","LEWIS","OLIVER","DAVID","ROBERT","JAMIE","NATHAN","CONNOR","JONATHAN","HARRY","CALLUM","AARON","ASHLEY","BRADLEY","JACOB","KIERAN","SCOTT","SAM","JOHN","BEN","MOHAMMED","NICHOLAS","KYLE","CHARLES","MARK","SEAN","EDWARD","STEPHEN","RICHARD","ALEX","PETER","DOMINIC","JOE","REECE","LEE","RHYS","STEVEN","ANTHONY","CHARLIE","PAUL","CRAIG","JASON","DALE","ROSS","CAMERON","LOUIS","DEAN","CONOR","SHANE","ELLIOT","PATRICK","MAX","SHAUN","HENRY","SIMON","TIMOTHY","MITCHELL","BILLY","PHILIP","JOEL","JOSH","MARCUS","DYLAN","CARL","ELLIOTT","BRANDON","MARTIN","TOBY","STUART","GARETH","DANNY","CHRISTIAN","TOM","DECLAN","KARL","MOHAMMAD","MATHEW","JAY","OWEN","DARREN" };

std::array<std::string, 256> nume_filme{ "The Shawshank Redemption","The Godfather","The Godfather: Part II","Pulp Fiction","The Good, the Bad and the Ugly","The Dark Knight","12 Angry Men","Schindler's List","The Lord of the Rings: The Return of the King","Fight Club","Star Wars: Episode V - The Empire Strikes Back","The Lord of the Rings: The Fellowship of the Ring","One Flew Over the Cuckoo's Nest","Goodfellas","Seven Samurai","Inception","Star Wars","Forrest Gump","The Matrix","The Lord of the Rings: The Two Towers","City of God","The Silence of the Lambs","Se7en","Once Upon a Time in the West","Casablanca","The Usual Suspects","Raiders of the Lost Ark","Rear Window","It's a Wonderful Life","Psycho","Léon: The Professional","Sunset Blvd.","American History X","Apocalypse Now","Terminator 2: Judgment Day","Memento","Saving Private Ryan","City Lights","Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb","Alien","Modern Times","Spirited Away","Gravity","North by Northwest","Back to the Future","Citizen Kane","The Pianist","M","Life Is Beautiful","The Shining","The Departed","Paths of Glory","Vertigo","American Beauty","Django Unchained","Double Indemnity","Taxi Driver","The Dark Knight Rises","Aliens","The Green Mile","The Intouchables","Gladiator","WALL·E","The Lives of Others","Toy Story 3","The Great Dictator","A Clockwork Orange","The Prestige","Amélie","Lawrence of Arabia","To Kill a Mockingbird","Reservoir Dogs","Das Boot","Cinema Paradiso","The Lion King","The Treasure of the Sierra Madre","The Third Man","Once Upon a Time in America","Requiem for a Dream","Star Wars: Episode VI - Return of the Jedi","Eternal Sunshine of the Spotless Mind","Full Metal Jacket","Braveheart","L.A. Confidential","Oldboy","Singin' in the Rain","Metropolis","Chinatown","Rashomon","Some Like It Hot","Bicycle Thieves","All About Eve","Monty Python and the Holy Grail","Princess Mononoke","Amadeus","2001: A Space Odyssey","Witness for the Prosecution","The Apartment","The Sting","Unforgiven","Grave of the Fireflies","Indiana Jones and the Last Crusade","Raging Bull","The Bridge on the River Kwai","Die Hard","Yojimbo","Batman Begins","A Separation","Inglourious Basterds","For a Few Dollars More","Mr. Smith Goes to Washington","Snatch.","Toy Story","On the Waterfront","The Great Escape","Downfall","Pan's Labyrinth","Up","The General","The Seventh Seal","Heat","The Elephant Man","The Maltese Falcon","The Kid","Blade Runner","Wild Strawberries","Rebecca","Scarface","Ikiru","Ran","Fargo","Gran Torino","Touch of Evil","The Big Lebowski","The Gold Rush","The Deer Hunter","Cool Hand Luke","It Happened One Night","Diabolique","No Country for Old Men","The Sixth Sense","Lock, Stock and Two Smoking Barrels","Jaws","Good Will Hunting","Strangers on a Train","Casino","Judgment at Nuremberg","The Grapes of Wrath","The Wizard of Oz","Platoon","Sin City","Butch Cassidy and the Sundance Kid","Kill Bill: Vol. 1","The Thing","Trainspotting","Gone with the Wind","High Noon","Annie Hall","Hotel Rwanda","The Hunt","Warrior","The Secret in Their Eyes","Finding Nemo","My Neighbor Totoro","V for Vendetta","Notorious","Dial M for Murder","The Avengers","How to Train Your Dragon","Life of Brian","Into the Wild","The Best Years of Our Lives","Network","The Terminator","Million Dollar Baby","There Will Be Blood","Ben-Hur","The Night of the Hunter","The Big Sleep","The King's Speech","Stand by Me","The 400 Blows","Twelve Monkeys","Groundhog Day","Donnie Darko","Dog Day Afternoon","Amores Perros","Howl's Moving Castle","Mary and Max","Gandhi","The Bourne Ultimatum","A Beautiful Mind","Persona","The Killing","The Graduate","Rush","Black Swan","The Princess Bride","Who's Afraid of Virginia Woolf?","The Hustler","The Man Who Shot Liberty Valance","La Strada","Anatomy of a Murder","8½","The Manchurian Candidate","Rocky","The Exorcist","Slumdog Millionaire","In the Name of the Father","Stalag 17","Rope","The Wild Bunch","Barry Lyndon","Monsters, Inc.","Fanny and Alexander","Infernal Affairs","The Truman Show","Roman Holiday","Life of Pi","Pirates of the Caribbean: The Curse of the Black Pearl","Memories of Murder","All Quiet on the Western Front","Harry Potter and the Deathly Hallows: Part 2","Sleuth","Stalker","Jurassic Park","A Streetcar Named Desire","Star Trek","Ratatouille","Ip Man","A Fistful of Dollars","The Diving Bell and the Butterfly","The Hobbit: An Unexpected Journey","District 9","Shutter Island","Rain Man","Incendies","Rosemary's Baby","La Haine","3 Idiots","The Artist","Nausicaä of the Valley of the Wind","Beauty and the Beast","Three Colors: Red","Bringing Up Baby","Mystic River","In the Heat of the Night","Arsenic and Old Lace","Before Sunrise","Papillon" };



size_t gen_between(size_t minim, size_t maxim) {

	return rand() % (maxim - minim + 1) + minim;

}

struct pub {

	std::string numeFilm;
	std::string autorFilm;
	size_t anFilm;
	unsigned char nota;
	int voturi;

};

pub * gen_pubs() {

	pub *publicatii = new pub[MAX_PUB];

	for (int i = 0; i< MAX_PUB; i++) {
		publicatii[i].autorFilm = nume_autori[gen_between(0, 100)];
		publicatii[i].numeFilm = nume_filme[i];
		publicatii[i].anFilm = gen_between(AN_MIN, AN_MAX);
		publicatii[i].voturi = gen_between(0, VOTURI_MAX);
		publicatii[i].nota = gen_between(0, 10);
	}

	return publicatii;
}

std::string print_pubs(pub publication) {

	std::string row = "{\"PUB\":\"" + PUBID + "\",";
	row += "\"nume_film\":\"" + publication.numeFilm + "\",";
	row += "\"autor_film\":\"" + publication.autorFilm + "\",";
	row += "\"an_film\":\"" + std::to_string(publication.anFilm) + "\",";
	row += "\"voturi_film\":\"" + std::to_string(publication.voturi) + "\",";
	row += "\"nota_film\":\"" + std::to_string(publication.nota) + "\"}";

	return row;
}

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

std::string generateID(){

	srand(time(NULL));
	int number = rand() % 900 + 100;
	return std::to_string(number);
};

std::string generateCON(std::string PUBID) {

	int i, j = 0;
	char buf[MAXDATASIZE];
	char part1[MAXDATASIZE] = "{\"TIP\":\"PUB\",\"ID\":\"";
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

void generatePubFeed() {

	pub *pubs = nullptr;
	int i = 0;

	while (true) {

		pubs = gen_pubs();

		for (i = 0; i < MAX_PUB; i++) {
			publicationList.push_back(print_pubs(pubs[i]));
		}
		delete[] pubs;

		std::this_thread::sleep_for(std::chrono::milliseconds(3000));

		PUB_Mutex.lock();
		publicationList.clear();
		PUB_Mutex.unlock();

	}

}

void sendPubFeed(int client) {

	int sentBytes = 0;
	while (true) {
		//std::cout << "Still here" << '\n';
		if (!publicationList.empty()) {
			PUB_Mutex.lock();
			for (auto itr = publicationList.begin(), end_itr = publicationList.end(); itr != end_itr; ++itr) {
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
				if (!(*itr).empty()) {
					std::cout << (*itr).c_str() << "\n";
					if ((*itr).length() > 255) {
						std::cout << "WTF!!" << '\n';
					}
					if ((sentBytes = send(client, (*itr).c_str(), PUBSIZE - 1, 0)) == -1) {
						perror("send");
						std::cout << WSAGetLastError() << '\n';
						PUB_Mutex.unlock();
						return;
					}
				}
			//	std::cout << (*itr).c_str() << "\n";
			}
			PUB_Mutex.unlock();
		}
	}

}

int syncronizeCON(const char * ip, const char * port, std::string message) {

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

	inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
		s, sizeof s);
	printf("Publisher: syncronizing with %s\n", s);

	freeaddrinfo(servinfo); // all done with this structure
	if ((numbytes = send(sockfd, message.c_str(), MAXDATASIZE - 1, 0)) == -1) {
		perror("send");
		exit(1);
	}

	printf("Syncronizing done...\n");
	closesocket(sockfd);
	return 0;

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
			printf("Pub: new connection from %s on "
			"socket %d\n",
			inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr*)&remoteaddr), remoteIP, INET6_ADDRSTRLEN),
			newfd);
	}

	return newfd;
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

		printf("Adresa locala este: %s si portul 60%s \n", str, PUBID.c_str());

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
						std::thread t2(sendPubFeed, newfd); t2.detach();
						FD_SET(newfd, &master); // add to master set
						if (newfd > fdmax)  // keep track of the max
							fdmax = newfd;
					}
				}
				else {
						//IGNORE NEW DATA FOR NOW
						std::cout << "IDK WHY GOT DATA FROM BROKER" << i << std::endl;
						closesocket(i); // bye!
						FD_CLR(i, &master); // remove from master set
					}

				} // END handle data from client
			} // END got new incoming connection
		} // END looping through file descriptors
	}

int main()
{
	PUBID = generateID();
	std::string CON_MSG = generateCON(PUBID);
	std::shuffle(nume_filme.begin(), nume_filme.end(), std::default_random_engine(seed));

	for (int i = 0; i< MAX_PUB; i++) {
		if (nume_filme[i].empty()) {
			nume_filme[i] = "A" + std::to_string(i);
		}
	}

	WSAInit();

	//ADD PUB TO THE LIST OF PUBS
	if (syncronizeCON(SYNC_HOSTNAME, SYNC_PORT, CON_MSG) != 0) {
		return -1;
	}

	std::thread t1(generatePubFeed); t1.detach();
	std::string pub_port = PORT_START + PUBID;
	Listener(pub_port.c_str());

    return 0;
}

