// PUB_SUB_Subscriber.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "winsock2.h"
#include <ws2tcpip.h>
#include <iostream>
#include <map>
#include <list>
#include <array>
#include <mutex>
#include <random>
#include <conio.h>
#include <string>
#include <cstdlib>
#include <time.h>
#include "json.hpp"

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
#define SUBSIZE 512
#define PUBSIZE 256
#define MAXLISTSIZE 1024
#define ever (;;)
using json = nlohmann::json;

struct sub {

	std::string numeFilm;
	std::string op_nume;

	std::string autorFilm;
	std::string op_autor;

	size_t anFilm = 0;
	std::string op_an;

	unsigned char nota = '\0';
	std::string op_nota;

	int voturi = 0;
	std::string op_voturi;

};

std::list < std::string> matchedSubscriptions;

std::string nume_autori[101] = { "THOMAS","JAMES","JACK","DANIEL","MATTHEW","RYAN","JOSHUA","LUKE","SAMUEL","JORDAN","ADAM","MICHAEL","ALEXANDER","CHRISTOPHER","BENJAMIN","JOSEPH","LIAM","JAKE","WILLIAM","ANDREW","GEORGE","LEWIS","OLIVER","DAVID","ROBERT","JAMIE","NATHAN","CONNOR","JONATHAN","HARRY","CALLUM","AARON","ASHLEY","BRADLEY","JACOB","KIERAN","SCOTT","SAM","JOHN","BEN","MOHAMMED","NICHOLAS","KYLE","CHARLES","MARK","SEAN","EDWARD","STEPHEN","RICHARD","ALEX","PETER","DOMINIC","JOE","REECE","LEE","RHYS","STEVEN","ANTHONY","CHARLIE","PAUL","CRAIG","JASON","DALE","ROSS","CAMERON","LOUIS","DEAN","CONOR","SHANE","ELLIOT","PATRICK","MAX","SHAUN","HENRY","SIMON","TIMOTHY","MITCHELL","BILLY","PHILIP","JOEL","JOSH","MARCUS","DYLAN","CARL","ELLIOTT","BRANDON","MARTIN","TOBY","STUART","GARETH","DANNY","CHRISTIAN","TOM","DECLAN","KARL","MOHAMMAD","MATHEW","JAY","OWEN","DARREN" };

std::array<std::string, 256> nume_filme{ "The Shawshank Redemption","The Godfather","The Godfather: Part II","Pulp Fiction","The Good, the Bad and the Ugly","The Dark Knight","12 Angry Men","Schindler's List","The Lord of the Rings: The Return of the King","Fight Club","Star Wars: Episode V - The Empire Strikes Back","The Lord of the Rings: The Fellowship of the Ring","One Flew Over the Cuckoo's Nest","Goodfellas","Seven Samurai","Inception","Star Wars","Forrest Gump","The Matrix","The Lord of the Rings: The Two Towers","City of God","The Silence of the Lambs","Se7en","Once Upon a Time in the West","Casablanca","The Usual Suspects","Raiders of the Lost Ark","Rear Window","It's a Wonderful Life","Psycho","Léon: The Professional","Sunset Blvd.","American History X","Apocalypse Now","Terminator 2: Judgment Day","Memento","Saving Private Ryan","City Lights","Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb","Alien","Modern Times","Spirited Away","Gravity","North by Northwest","Back to the Future","Citizen Kane","The Pianist","M","Life Is Beautiful","The Shining","The Departed","Paths of Glory","Vertigo","American Beauty","Django Unchained","Double Indemnity","Taxi Driver","The Dark Knight Rises","Aliens","The Green Mile","The Intouchables","Gladiator","WALL·E","The Lives of Others","Toy Story 3","The Great Dictator","A Clockwork Orange","The Prestige","Amélie","Lawrence of Arabia","To Kill a Mockingbird","Reservoir Dogs","Das Boot","Cinema Paradiso","The Lion King","The Treasure of the Sierra Madre","The Third Man","Once Upon a Time in America","Requiem for a Dream","Star Wars: Episode VI - Return of the Jedi","Eternal Sunshine of the Spotless Mind","Full Metal Jacket","Braveheart","L.A. Confidential","Oldboy","Singin' in the Rain","Metropolis","Chinatown","Rashomon","Some Like It Hot","Bicycle Thieves","All About Eve","Monty Python and the Holy Grail","Princess Mononoke","Amadeus","2001: A Space Odyssey","Witness for the Prosecution","The Apartment","The Sting","Unforgiven","Grave of the Fireflies","Indiana Jones and the Last Crusade","Raging Bull","The Bridge on the River Kwai","Die Hard","Yojimbo","Batman Begins","A Separation","Inglourious Basterds","For a Few Dollars More","Mr. Smith Goes to Washington","Snatch.","Toy Story","On the Waterfront","The Great Escape","Downfall","Pan's Labyrinth","Up","The General","The Seventh Seal","Heat","The Elephant Man","The Maltese Falcon","The Kid","Blade Runner","Wild Strawberries","Rebecca","Scarface","Ikiru","Ran","Fargo","Gran Torino","Touch of Evil","The Big Lebowski","The Gold Rush","The Deer Hunter","Cool Hand Luke","It Happened One Night","Diabolique","No Country for Old Men","The Sixth Sense","Lock, Stock and Two Smoking Barrels","Jaws","Good Will Hunting","Strangers on a Train","Casino","Judgment at Nuremberg","The Grapes of Wrath","The Wizard of Oz","Platoon","Sin City","Butch Cassidy and the Sundance Kid","Kill Bill: Vol. 1","The Thing","Trainspotting","Gone with the Wind","High Noon","Annie Hall","Hotel Rwanda","The Hunt","Warrior","The Secret in Their Eyes","Finding Nemo","My Neighbor Totoro","V for Vendetta","Notorious","Dial M for Murder","The Avengers","How to Train Your Dragon","Life of Brian","Into the Wild","The Best Years of Our Lives","Network","The Terminator","Million Dollar Baby","There Will Be Blood","Ben-Hur","The Night of the Hunter","The Big Sleep","The King's Speech","Stand by Me","The 400 Blows","Twelve Monkeys","Groundhog Day","Donnie Darko","Dog Day Afternoon","Amores Perros","Howl's Moving Castle","Mary and Max","Gandhi","The Bourne Ultimatum","A Beautiful Mind","Persona","The Killing","The Graduate","Rush","Black Swan","The Princess Bride","Who's Afraid of Virginia Woolf?","The Hustler","The Man Who Shot Liberty Valance","La Strada","Anatomy of a Murder","8½","The Manchurian Candidate","Rocky","The Exorcist","Slumdog Millionaire","In the Name of the Father","Stalag 17","Rope","The Wild Bunch","Barry Lyndon","Monsters, Inc.","Fanny and Alexander","Infernal Affairs","The Truman Show","Roman Holiday","Life of Pi","Pirates of the Caribbean: The Curse of the Black Pearl","Memories of Murder","All Quiet on the Western Front","Harry Potter and the Deathly Hallows: Part 2","Sleuth","Stalker","Jurassic Park","A Streetcar Named Desire","Star Trek","Ratatouille","Ip Man","A Fistful of Dollars","The Diving Bell and the Butterfly","The Hobbit: An Unexpected Journey","District 9","Shutter Island","Rain Man","Incendies","Rosemary's Baby","La Haine","3 Idiots","The Artist","Nausicaä of the Valley of the Wind","Beauty and the Beast","Three Colors: Red","Bringing Up Baby","Mystic River","In the Heat of the Night","Arsenic and Old Lace","Before Sunrise","Papillon" };

std::string operatori[5] = { "<=", "<", ">=", ">", "=" };

unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();

size_t gen_between(size_t minim, size_t maxim) {

	return rand() % (maxim - minim + 1) + minim;

}

sub * gen_subs(int numar_subscriptii, int procent_film_exact, int procent_autor_exact, int procent_an_exact, int procent_voturi_exact, int procent_nota_exact, int procent_operator_nota) {

	// OPERATORI -2 {<=}, -1 {<}, 0 {=}, 1 {>}, 2 {>=}

	int i = 0;
	int procent_film = int((double(procent_film_exact) / 100) * numar_subscriptii);
	int procent_autor = int((double(procent_autor_exact) / 100) * numar_subscriptii);
	int procent_an = int((double(procent_an_exact) / 100) * numar_subscriptii);
	int procent_voturi = int((double(procent_voturi_exact) / 100) * numar_subscriptii);
	int procent_nota = int((double(procent_nota_exact) / 100) * numar_subscriptii);
	//pentru x din procent_nota este valabila conditia.
	int procent_op_nota = int((double(procent_operator_nota) / 100) * procent_nota);

	sub *subscriptii = new sub[numar_subscriptii];


	while (procent_film) {
		subscriptii[i].numeFilm = nume_filme[gen_between(0, 255)];
		subscriptii[i].op_nume = operatori[4];
		procent_film--;
		i++;
	}

	if (i > numar_subscriptii) {
		std::shuffle(subscriptii, subscriptii + numar_subscriptii, std::default_random_engine(seed));
		i = 0;
	}

	while (procent_autor) {
		subscriptii[i].autorFilm = nume_autori[gen_between(0, 100)];
		subscriptii[i].op_autor = operatori[4];
		procent_autor--;
		i++;
	}


	if (i > numar_subscriptii) {
		std::shuffle(subscriptii, subscriptii + numar_subscriptii, std::default_random_engine(seed));
		i = 0;
	}
	while (procent_an) {
		subscriptii[i].anFilm = gen_between(AN_MIN, AN_MAX);
		subscriptii[i].op_an = operatori[gen_between(0, 4)];
		procent_an--;
		i++;
	}


	if (i > numar_subscriptii) {
		std::shuffle(subscriptii, subscriptii + numar_subscriptii, std::default_random_engine(seed));
		i = 0;
	}
	while (procent_voturi) {
		subscriptii[i].voturi = gen_between(0, VOTURI_MAX);
		subscriptii[i].op_voturi = operatori[gen_between(0, 4)];
		procent_voturi--;
		i++;
	}

	if (i > numar_subscriptii) {
		std::shuffle(subscriptii, subscriptii + numar_subscriptii, std::default_random_engine(seed));
		i = 0;
	}
	while (procent_nota) {
		if (procent_op_nota > 0) {
			subscriptii[i].op_nota = operatori[4];
			procent_op_nota--;
		}
		else {
			subscriptii[i].nota = gen_between(0, 10);
			subscriptii[i].op_nota = operatori[gen_between(0, 3)];
		}
		procent_nota--;
		i++;
	}
	std::shuffle(subscriptii, subscriptii + numar_subscriptii, std::default_random_engine(seed));

	return subscriptii;
}

std::string print_subs(sub subscription) {


	// {"nume_film":{"operator":"=","valoare":"Alibaba"},"autor_film":{"operator":"=","valoare":"Jack"},"an_film":{"operator":">","valoare":"12"},"voturi_film":{"operator":"<","valoare":"9000"},"nota_film":{"operator":"=","valoare":"4"}}

	std::string row = "{\"nume_film\":{\"operator\":\"" + subscription.op_nume + "\",\"valoare\":\"" + subscription.numeFilm + "\"},";
	row += "\"autor_film\":{\"operator\":\"" + subscription.op_autor + "\",\"valoare\":\"" + subscription.autorFilm + "\"},";
	row += "\"an_film\":{\"operator\":\"" + subscription.op_an + "\",\"valoare\":\"" + std::to_string(subscription.anFilm) + "\"},";
	row += "\"voturi_film\":{\"operator\":\"" + subscription.op_voturi + "\",\"valoare\":\"" + std::to_string(subscription.voturi) + "\"},";
	row += "\"nota_film\":{\"operator\":\"" + subscription.op_nota + "\",\"valoare\":\"" + std::to_string(subscription.nota) + "\"}}";

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

int syncronizeCON(const char * ip, const char * port, std::string message, std::string &response = std::string(), int type = 0) {

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
	else if (type == 1) {
		int data;
		char buffer[MAXLISTSIZE];
		freeaddrinfo(servinfo); // all done with this structure
		if ((numbytes = send(sockfd, message.c_str(), MAXDATASIZE - 1, 0)) == -1) {
			perror("send");
			exit(1);
		}
		if ((data = recv(sockfd, buffer, MAXLISTSIZE - 1, 0)) <= 0) {
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
		printf("Got brokers list\n");
		response.assign(buffer, buffer + MAXLISTSIZE - 1);
	}
	else {


	}
	closesocket(sockfd);
	return 0;

}

int connectToBroker(std::map<std::string, std::string> ips_ports) {


	std::map<std::string, std::string>::iterator it;

	for (it = ips_ports.begin(); it != ips_ports.end(); it++)
	{

		std::string ip = it->second;
		std::string port = "60" + it->first;
		std::cout << port.c_str() << "\n";

		if (ip == "::1") {
			ip.clear();
			ip.assign("localhost");
		}

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
			continue;
		}
		if(sockfd > 0)
			return sockfd;
		return 0;
	}
}

std::map<std::string, std::string> getBrokerList() {

	std::string CON_MSG = "{\"TIP\":\"BRO\",\"ID\":\"100\",\"MSG\":\"LSB\"}";
	std::string message;
	std::map<std::string, std::string> BROKER_INFO;
	if (syncronizeCON(SYNC_HOSTNAME, SYNC_PORT, CON_MSG, message, 1) != 0) {
		return{};
	}
	json js = json::parse(message);

	for (json::iterator it = js.begin(); it != js.end(); ++it) {
		std::cout << *it << '\n';
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

void registerSubscriptions(sub *subs, int broker) {

	int data, sentBytes;
	char buffer[PUBSIZE];

	for (int i = 0; i < MAX_PUB; i++) {

		std::string subscription = print_subs(subs[i]);

		if ((sentBytes = send(broker, print_subs(subs[i]).c_str(), SUBSIZE - 1, 0)) == -1) {
			perror("send");
			std::cout << WSAGetLastError() << '\n';
			return;
		}

	}

	for ever {


		if ((data = recv(broker, buffer, PUBSIZE - 1, 0)) <= 0) {
			// got error or connection closed by client
			if (data == 0) {
				// connection closed
				printf("Broker feed  hung up\n");
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
				matchedSubscriptions.push_back(str);
				std::cout << str.c_str() << "\n";
				//std::cout << (matchedSubscriptions.end())->c_str() << "\n";


			}
		}
	}

}


int main()
{
	int nr_subs = MAX_PUB;
	std::map<std::string, std::string> brolist;

	std::shuffle(nume_filme.begin(), nume_filme.end(), std::default_random_engine(seed));

	sub *subs = nullptr;

	for (int i = 0; i< MAX_PUB; i++) {
		if (nume_filme[i].empty()) {
			nume_filme[i] = "A" + std::to_string(i);
			// std::cout << nume_filme[i] << "\n";
		}
	}
	subs = gen_subs(nr_subs, 20, 20, 20, 20, 20, 30);

	WSAInit();


	do {

		std::this_thread::sleep_for(std::chrono::seconds(2));
		brolist = getBrokerList();	

	} while (brolist.empty());

	int brosock = connectToBroker(brolist);
	registerSubscriptions(subs, brosock);

	delete[] subs;
	return 0;
}

