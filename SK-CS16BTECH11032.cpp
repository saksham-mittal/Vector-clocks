/*

The following code is an implementation of 'Singhal–Kshemkalyani’s differential technique' for vector time
in distributed systems.

Socket programming is used to stimulate the distributed system.

To run this file :
g++ -std=c++11 -pthread SK-CS16BTECH11032.cpp -o SK-CS16BTECH11032

To execute :
./SK-CS16BTECH11032

*/
#include <iostream>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>      //
#include <sys/socket.h>     //      These libraries are used
#include <netinet/in.h>     //      for Socket prog.
#include <arpa/inet.h>      //

#include <thread>           //      This is the thread library
#include <random>           //      For generating exponential distribution
#include <chrono>           //      For measuring time in milliseconds
#include <mutex>            //      For mutex locks
#include <ctime>            //      For measuring the current system's time
#include <vector>           //      STL library for vectors
#include <fstream>          //      For reading/writing files

using namespace std;

ofstream logFile;

int n, lambda, m, x;
double alpha;

vector <int> *g;

std::mutex mtx, mtx_wait;

int internal_evnts, send_evnts, total_evnts;
int re;

int serv_wait = 0;

int no_tuples_sent = 0;

double run_exp(float lmabda) {
    default_random_engine generate;
    exponential_distribution <double> distribution(1.0/lambda);
    return distribution(generate);
}

void error(const char* msg) {
    perror(msg);
    exit(1);
}

void sender(int id, int *clck, int *LS, int *LU) {
    /*
    Sender function first deciedes which nodes to connect using the 
    adjacency list. Then it sends a request using the port no. 4000 + (node_no)
    After the connection is established, it writes(sends) the msg
    */

    // this_thread::sleep_for(chrono::milliseconds(3000));

    vector<int> portno;
    for(int i=0; i<g[id].size(); i++)
        portno.push_back(4000 + g[id][i]);


    int sockfd[g[id].size()], n_tmp;
    char *msg;


    // Calculating probability of internal event
    double probability_intrn_evnt = (double)internal_evnts/(double)(internal_evnts + send_evnts);
    // printf("Probability of internal event = %e\n", probability_intrn_evnt);
    int ie = internal_evnts, se = send_evnts;
    
    // Busy wait till all the receivers start listening
    while(serv_wait < n);

    for(int i=0; i<g[id].size(); i++) {
        struct sockaddr_in serv_addr;
        sockfd[i] = socket(AF_INET, SOCK_STREAM, 0);   // We are using TCP protocol
        // printf("Socket created by sender\n");

        if(sockfd[i] < 0) {
            error("Error opening Socket.");
        }

        memset(&serv_addr, '0', sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        
        int portNum = portno[i];
        serv_addr.sin_port = htons(portNum);

        if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
	        error("Invalid address/Address not supported.");
	    }

        if(connect(sockfd[i], (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
            error("Connection falied.");
        }
        // Connection is established. Sender can now send msgs
        // printf("Conn estabilished by sender\n");
    }

    for(int i=0; i<total_evnts; i++) {
        if(ie > 0 && se > 0) {
            srand(time(NULL));
            double r = ((double) rand() / (RAND_MAX));
            // printf("r = %e\n", r);
            if(r >= probability_intrn_evnt) {
                // Internal event happens
                ie--;
                // Print that internal event is excecuted

                mtx.lock();

                time_t now = time(0);
                tm *time_var = localtime(&now);
                
                // Vector clock of the process id's index is incremented
                clck[id] += 1;
                // Increment the LU vector
                LU[id] += 1;

                logFile << "Process" << id + 1 << " excecutes internal event e" << id + 1 << i + 1 << " at " << time_var->tm_min << ":" << time_var->tm_sec << ", vc: [";
                for(int i=0; i<n-1; i++)
                    logFile << clck[i] << " ";
                logFile <<  clck[n - 1] << "]\n";
                
                mtx.unlock();
            } else {
                se--;

                // Select a process from adjacency list on random
                // Send the vector clock to that process
                
                // The random number is to be selected from 1 to g[id].size()
                srand(time(NULL));
                int rndNum = (rand() % g[id].size());

                // th_id is the thread id to which the msg is sent
                int th_id = portno[rndNum] - 3999;

                mtx.lock();

                time_t now = time(0);
                tm *time_var = localtime(&now);

                // Increment its clock before sending
                clck[id] += 1;
                // Increment the LU vector
                LU[id] += 1;

                // The msg sent is the tuples concatenated with the id
                string temp_convert = "";

                // Compare the LS[rndNum] to all of LU and then send the tuples
                for(int k=0; k<n; k++) {
                    if(LS[rndNum] < LU[k]) {
                        no_tuples_sent++;
                        temp_convert += ("(" + to_string(k) + " " + to_string(clck[k]) + ")");
                    }
                }
                // Concatenatenating the id and msg number
                temp_convert += ("| " + to_string(id + 1) + "| " + to_string(i + 1) + ";");

                // Sample msg sent : (2 3)(5 4)(1 10)| 2| 3;...

                msg = (char *)temp_convert.c_str();
                // printf("msg = %s\n", msg);
                n_tmp = write(sockfd[rndNum], msg, strlen(msg));
                if(n_tmp < 0) {
                    error("Error on writing.");
                    exit(1);
                }

                // Update the LS vector (i.e. the rndNum'th index)
                LS[rndNum] = clck[id];

                // Print message sent in the format
                logFile << "Process" << id + 1 << " sends message m" << id + 1 << i + 1 << " to process" << th_id << " at " << time_var->tm_min << ":" << time_var->tm_sec << ", vc: [";
                for(int i=0; i<n-1; i++)
                    logFile << clck[i] << " ";
                logFile <<  clck[n - 1] << "]\n";

                mtx.unlock();
            }
        } else {
            if(ie == 0) {
                // Only send event happens
                se--;
                
                // Select a process from adjacency list on random
                // Send the vector clock to that process
                
                // The random number is to be selected from 1 to g[id].size()
                srand(time(NULL));
                int rndNum = (rand() % g[id].size());

                // th_id is the thread id to which the msg is sent
                int th_id = portno[rndNum] - 3999;

                mtx.lock();

                time_t now = time(0);
                tm *time_var = localtime(&now);

                // Increment its clock before sending
                clck[id] += 1;
                // Increment the LU vector
                LU[id] += 1;

                // The msg sent is the tuples concatenated with the id
                string temp_convert = "";

                // Compare the LS[rndNum] to all of LU and then send the tuples
                for(int k=0; k<n; k++) {
                    if(LS[rndNum] < LU[k]) {
                        no_tuples_sent++;
                        temp_convert += ("(" + to_string(k) + " " + to_string(clck[k]) + ")");
                    }
                }
                temp_convert += ("| " + to_string(id + 1) + "| " + to_string(i + 1) + ";");

                // Sample msg sent : (2 3)(5 4)(1 10)| 2| 3;...

                msg = (char *)temp_convert.c_str();
                // printf("msg = %s\n", msg);
                n_tmp = write(sockfd[rndNum], msg, strlen(msg));
                if(n_tmp < 0) {
                    error("Error on writing.");
                    exit(1);
                }

                // Update the LS vector (i.e. the rndNum'th index)
                LS[rndNum] = clck[id];

                // Print message sent in the format
                logFile << "Process" << id + 1 << " sends message m" << id + 1 << i + 1 << " to process" << th_id << " at " << time_var->tm_min << ":" << time_var->tm_sec << ", vc: [";
                for(int i=0; i<n-1; i++)
                    logFile << clck[i] << " ";
                logFile <<  clck[n - 1] << "]\n";
                
                mtx.unlock();
            } else {
                // Only internal event happens
                ie--;

                mtx.lock();

                time_t now = time(0);
                tm *time_var = localtime(&now);

                // Vector clock of the process id's index is incremented
                clck[id] += 1;
                // Increment the LU vector
                LU[id] += 1;

                logFile << "Process" << id + 1 << " excecutes internal event e" << id + 1 << i + 1 << " at " << time_var->tm_min << ":" << time_var->tm_sec << ", vc: [";
                for(int i=0; i<n-1; i++)
                    logFile << clck[i] << " ";
                logFile <<  clck[n - 1] << "]\n";
                
                mtx.unlock();
            }
        }
        
        // Generating delay that is exponentially distributed with inter-event time λ ms
        this_thread::sleep_for(chrono::milliseconds((int)run_exp(lambda)));
    }

    for(int i=0; i<g[id].size(); i++)
        close(sockfd[i]);
}

void receiver(int id, int *clck, int *LU) {
    /*
    Receiver creates the socket and listens for connections
    When a connection is estabilished, it receives a msg
    */
    int max_conn = 0, opt = 1;
    char buffer[2000];

    for(int i=0; i<n; i++) {
        for(int j=0; j<g[i].size(); j++) {
            if(g[i][j] == id) {
                max_conn++;
            }
        }
    }

    int sockfd, newsockfd[max_conn], portno, c, n_tmp, sck_tmp;

    struct sockaddr_in serv_addr, cli_addr;
    socklen_t clilen;

    // Create the socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);   // We are using TCP protocol
    if(sockfd < 0) {
        // Socket could not be created
        error("Error opening Socket.");
    }
    // printf("Socket created by receiver\n");

    bzero((char*) &serv_addr, sizeof(serv_addr));   // Clearing the serv_addrr
    portno = 4000 + id;

    // Forcefully attaching socket to the port 8080
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    // Preparing the sockaddr_in structure
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);

    // Binding the socket
    if(bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        // Socket could not be binded
        error("Binding Failed.");
    }

    // Socket listening
    listen(sockfd, max_conn);  // max_conn = Max threads that can send msg to receiver

    mtx_wait.lock();
    serv_wait++;
    mtx_wait.unlock();

    // Accept incoming connections
    clilen = sizeof(cli_addr);

    int cnt_conn = 0;
    while(cnt_conn < max_conn) {
        if(sck_tmp = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen)) {
            newsockfd[cnt_conn] = sck_tmp;
            cnt_conn++;
        }
    }

    bool lp_break = true;
    while(lp_break) {
        for(int i=0; i<max_conn; i++) {
            bzero(buffer, 2000);
            n_tmp = read(newsockfd[i], buffer, 2000);
            if(n_tmp < 0) {
                error("Error on reading.");
            }
            if(strlen(buffer) > 0) {
                mtx.lock();

                // Extract the sender here (Traverse the buffer)
                // Sample buffer : "(2 3)(5 4)(1 10)| 2| 3;..."
                int ind = 0;
                while(buffer[ind] != '\0') {
                    char str;
                    vector<int> indcies, vals;
                    int var, thread_send, msg_send;

                    // Parsing till the '|'
                    while(buffer[ind] != '|') {
                        var = 0;
                        while(buffer[ind] != ')') {
                            if(buffer[ind] != ' ' && buffer[ind] != '(') {
                                str = buffer[ind];
                                var = 10 * var + (int)(str - '0');
                                if(buffer[ind + 1] == ')') {
                                    vals.push_back(var);
                                    var = 0;
                                }
                            } else if(buffer[ind] == ' ') {
                                indcies.push_back(var);
                                var = 0;
                            }
                            ind++;
                        }
                        ind++;
                    }

                    ind += 2;
                    var = 0;
                    // Parsing the thread which sent this msg
                    while(buffer[ind] != '|') {
                        str = buffer[ind];
                        var = 10 * var + (int)(str - '0');
                        ind++;
                    }
                    thread_send = var;

                    ind += 2;
                    var = 0;
                    // Parsing the msg number
                    while(buffer[ind] != ';') {
                        str = buffer[ind];
                        var = 10 * var + (int)(str - '0');
                        ind++;
                    }
                    msg_send = var;

                    ind++;

                    time_t now = time(0);
                    tm *time_var = localtime(&now);

                    clck[id] += 1;
                    // Increment the LU vector
                    LU[id] += 1;

                    // Now update the clck[] according to the indcies[] and vals[]  
                    // to the clock of the process

                    for(int k=0; k<indcies.size(); k++) {
                        clck[indcies[k]] = max(clck[indcies[k]], vals[k]);
                        LU[indcies[k]] = clck[id];
                    }
                    // printf("---------Check---------\n");
                    // printf("The vector received : \n");
                    // for(int i=0; i<n; i++) {
                    //     printf("%d ", received_clck[i]);
                    // }
                    // printf("\nThe msg was sent by thread %d\n", thread_send);
                    // printf("-----------------------\n");

                    logFile << "Process" << id + 1 << " receives m" << thread_send << msg_send << " from process" << thread_send << " at " << time_var->tm_min << ":" << time_var->tm_sec << ", vc: [";
                    for(int i=0; i<n-1; i++)
                        logFile << clck[i] << " ";
                    logFile <<  clck[n - 1] << "], tuples: {";
                    for(int k=0; k<indcies.size()-1; k++)
                        logFile << "(" << indcies[k] + 1 << " " << vals[k] << "), ";
                    logFile << "(" << indcies[indcies.size() - 1] + 1 << " " <<vals[indcies.size() - 1] << ")}\n";

                    re--;

                }

                mtx.unlock();
            }
        }

        // lp_break = false means all receiver threads can now exit
        if(re == 0)
            lp_break = false;
    }
}

void manage(int id) {
    // This function creates 2 threads : sender and receiver
    // printf("Manage function called for thread no. %d\n", id + 1);

    // tm_clck[] is the vector clock of each process
    int *tm_clck;
    tm_clck = new int[n];

    // LS[] is the Last sent vector
    // LU[] is the Last updated vector
    int *LS, *LU;
    LS = new int[n];
    LU = new int[n];
    
    // All clocks(and LS, LU) are intialised with 0
    for(int i=0; i<n; i++)  {
        tm_clck[i] = 0;
        LS[i] = 0;
        LU[i] = 0;
    }

    thread rec_th(receiver, id, tm_clck, LU);
    thread sdr_th(sender, id, tm_clck, LS, LU);

    // printf("Threads created\n");

    sdr_th.join();
    rec_th.join();
}

int main(int argc, char const *argv[])
{
    string temp;
    
    ifstream inp_file;
    inp_file.open("inp-params.txt");

    // Parsing of the input file
    getline(inp_file, temp);
    int len = temp.length(); 
    char char_array[len + 1];
    strcpy(char_array, temp.c_str());
    char str;
    int var = 0, ind = 0, beta = 0, first_val;
    string tmp = "";
    while(char_array[ind] != '\0') {
        if(char_array[ind] != ' ') {
            str = char_array[ind];
            if(beta == 2) {
                tmp += str;
            } else {
                var = 10 * var + (int)(str - '0');
            }

            if(char_array[ind + 1] == '\0') {
                m = var;
                var = 0;
            }
        } else {
            beta++;
            if(beta == 1) {
                n = var;
            } else if(beta == 2) {
                lambda = var;
            } else if(beta == 3) {
                alpha = atof(tmp.c_str());
            }
            var = 0;
        }
        ind++;
    }

    // printf("n = %d, lambda = %d, alpha = %e, m = %d\n", n, lambda, alpha, m);

    internal_evnts = alpha * m;
    send_evnts = m;
    total_evnts = internal_evnts + send_evnts;

    re = send_evnts * n;

    // printf("internal_evnts = %d, send_evnts = %d, total_evnts = %d\n", internal_evnts, send_evnts, total_evnts);

    g = new vector<int>[n];

    while(getline(inp_file, temp)) {
        len = temp.length(); 
        char char_array[len + 1];
        strcpy(char_array, temp.c_str());

        var = 0, ind = 0, first_val, beta = 0;
        while(char_array[ind] != '\0') {
            if(char_array[ind] != ' ') {
                str = char_array[ind];
                var = 10 * var + (int)(str - '0');

                if(char_array[ind + 1] == '\0') {
                    g[first_val - 1].push_back(var - 1);
                    var = 0;
                }
            } else {
                beta++;
                if(beta == 1) {
                    first_val = var;
                } else {
                    g[first_val - 1].push_back(var - 1);
                }
                var = 0;
            }
            ind++;
        }
    }

    inp_file.close();
    // Parsing finished

    /*
    This is for checking the adjacency list(readed from inp-params.txt)

    for(int i=0; i<n; i++) {
        for(int j=0; j<g[i].size(); j++)
            cout << g[i][j] << " ";
        cout << endl;
    }
    
    */

    // Each node of the graph creates its own thread
    thread th_arr[n];

    logFile.open("Log-SK.txt");

    for(int i=0; i<n; i++) {
        th_arr[i] = thread(manage, i); 
    }

    // Threads are joined after completing all the events
    for(int i=0; i<n; i++) {
        th_arr[i].join();
    }

    logFile.close();

    printf("Total vc's sent = %d, and total tuples sent = %d\n", send_evnts * n * n, no_tuples_sent);

    return 0;
}
