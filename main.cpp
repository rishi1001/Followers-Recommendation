#include <iostream>
#include <fstream>
#include <string>
#include <mpi.h>
#include <omp.h>
#include <assert.h>
#include <algorithm>
#include <vector>
#include <queue>
#include <cmath>
#include <bits/stdc++.h>
#include <unordered_map>

using namespace std;

unsigned int L, D;
ifstream fs_ep, fs_index, fs_indptr, fs_level_offset, fs_max_level, fs_vect;

string trim(string& str) {
    str.erase(str.find_last_not_of(' ')+1);
    str.erase(0, str.find_first_not_of(' '));
    return str;
}

struct comp{
	bool operator()(pair<double,int> const& v1, pair<double,int> const& v2){
		return v1.first>v2.first;
	}
};

double cosine_dist(vector<double>& a1, vector<double>& a2) {
    assert(a1.size() == a2.size());
    int n = a1.size();
    double dot_prod = 0;
    double a1_norm = 0, a2_norm = 0;
    for(int i = 0; i < n; i++) {
        dot_prod += a1[i] * a2[i];
        a1_norm += a1[i] * a1[i];
        a2_norm += a2[i] * a2[i];
    }
    a1_norm = sqrt(a1_norm);
    a2_norm = sqrt(a2_norm);
    double cos_sim = dot_prod / (a1_norm * a2_norm);
    return 1 - cos_sim;
}

priority_queue<pair<double,int>,vector<pair<double,int>>,comp> searchLayer(int k, vector<double>& q, priority_queue<pair<double,int>,vector<pair<double,int>>,comp> candidates_temp, vector<int>& indptr, vector<int>& index, vector<int>& level_offset, int lc, unordered_map<int,int>& visited, vector<vector<double>>& vect) {
    priority_queue<pair<double,int>> topk;
    priority_queue<pair<double,int>,vector<pair<double,int>>,comp> candidates;
    while(!candidates_temp.empty()){
        pair<double,int> v = candidates_temp.top();
        candidates.push(v);
        topk.push(v);
        candidates_temp.pop();
    }
    while(!candidates.empty()) {
        int ep = candidates.top().second;
        candidates.pop();
        int start = indptr[ep] + level_offset[lc];
        int end = indptr[ep] + level_offset[lc+1];
        for(int i = start; i < end; i++) {
            int px = index[i];
            if(px == -1 || visited[px] == 1) continue;
            visited[px] = 1;
            double dist = cosine_dist(q, vect[px]);
            if(dist > topk.top().first && topk.size() == k) continue;
            topk.push({dist, px});
            if(topk.size() > k) topk.pop();
            candidates.push({dist, px});
        }
    }
    priority_queue<pair<double,int>,vector<pair<double,int>>,comp> topk_return;
    while(!topk.empty()){
        pair<double,int> v = topk.top();
        topk_return.push(v);
        topk.pop();
    }
    return topk_return;
}

priority_queue<pair<double, int>, vector<pair<double,int>>, comp> queryHNSW(vector<double>& q, int k, int ep, vector<int>& indptr, vector<int>& index, vector<int>& level_offset, int max_level, vector<vector<double>>& vect) {
    priority_queue<pair<double,int>,vector<pair<double,int>>,comp> topk;
    topk.push({cosine_dist(q, vect[ep]), ep});
    unordered_map<int, int> visited;
    visited[ep] = 1;
    for(int lc = max_level; lc >= 0; lc--) {
        // cout << "Level: " << lc << "\n";
        topk = searchLayer(k, q, topk, indptr, index, level_offset, lc, visited, vect);
    }
    return topk;
}

int main(int argc, char* argv[]) {
    auto begin = std::chrono::high_resolution_clock::now();
    assert(argc > 4);

    int k = stoi(argv[2]);

    fs_ep.open(string(argv[1]) + "ep.bin", ios::in | ios::binary);
    fs_index.open(string(argv[1]) + "index.bin", ios::in | ios::binary);
    fs_indptr.open(string(argv[1]) + "indptr.bin", ios::in | ios::binary);
    fs_level_offset.open(string(argv[1]) + "level_offset.bin", ios::in | ios::binary);
    fs_max_level.open(string(argv[1]) + "max_level.bin", ios::in | ios::binary);
    fs_vect.open(string(argv[1]) + "vect.bin", ios::in | ios::binary);
    
    unsigned int ep;
    fs_ep.read((char *) &ep, sizeof(unsigned int));
    
    unsigned int max_level;
    fs_max_level.read((char *) &max_level, sizeof(unsigned int));

    streampos fileSize;

    fs_level_offset.seekg(0, ios::end);
    fileSize = fs_level_offset.tellg();
    fs_level_offset.seekg(0, ios::beg);
    vector<int> level_offset(fileSize / sizeof(unsigned int));
    fs_level_offset.read((char *) level_offset.data(), fileSize);

    fs_indptr.seekg(0, ios::end);
    fileSize = fs_indptr.tellg();
    fs_indptr.seekg(0, ios::beg);
    vector<int> indptr(fileSize / sizeof(unsigned int));
    fs_indptr.read((char *) indptr.data(), fileSize);

    fs_index.seekg(0, ios::end);
    fileSize = fs_index.tellg();
    fs_index.seekg(0, ios::beg);
    vector<int> index(fileSize / sizeof(unsigned int));
    fs_index.read((char *) index.data(), fileSize);

    ifstream userFile1;
    userFile1.open(argv[3]);
    string line1;
    getline(userFile1, line1);
    line1 = trim(line1);
    for (auto x : line1) {
        if (x == ' ') D++;
    }
    D++;

    fs_vect.seekg(0, ios::end);
    fileSize = fs_vect.tellg();
    L = fileSize / (D * sizeof(double));
    vector<vector<double>> vect(L, vector<double> (D, 0.0));
    for(int i = 0; i < L; i++) {
        fs_vect.seekg(i * D * sizeof(double), ios::beg);
        fs_vect.read((char *) vect[i].data(), D * sizeof(double));
    }

    int rank, size;
    int temp;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &temp);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int C = 1;

    if(rank == 0) cout << "Starting program: " << size << " " << C << "\n";

    if(rank == 0) {
        ifstream userFile;
        userFile.open(argv[3]);

        ofstream fs_output; 
        fs_output.open(argv[4], ios::out);

        vector<vector<double>> users;
        string line;
        while(userFile) {
            vector<double> user(D);
            getline(userFile, line);
            line = trim(line);
            int i = 0;
            string word = "";
            for (auto x : line) {
                if (x == ' ') {
                    double val = stod(word);
                    user[i] = val;
                    i++;         
                    word = "";
                } else {
                    word = word + x;
                }
            }
            if(word != "") {
                double val = stod(word);
                user[i] = val;
                i++;
            }
            if(i != D) break;
            users.push_back(user);
            if(userFile.eof()) break;
        }

        vector<vector<int>> rank_user(size,vector<int> (C));
        map<int,vector<int>> final_answer;

        #pragma omp parallel for num_threads(C)
        for(int user_num = 0; user_num < 2; user_num++) {
            cout << "User Started: " << user_num << "\n";
            vector<double> user = users[user_num];
            vector<int> ans(k, -1);
            MPI_Status status;
            int flag = 0; 
            MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
            if(flag) {
                MPI_Recv(ans.data(), k, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);
                if(ans[0] != -1) {
                    int curr_user = rank_user[status.MPI_SOURCE][status.MPI_TAG];
                    final_answer[curr_user] = ans;
                }
                MPI_Send(user.data(), D, MPI_DOUBLE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                rank_user[status.MPI_SOURCE][status.MPI_TAG] = user_num;
            } else {
                priority_queue<pair<double, int>, vector<pair<double,int>>, comp> topk = queryHNSW(user, k, ep, indptr, index, level_offset, max_level, vect);
                int i = 0;
                assert(topk.size() == k);
                while(!topk.empty()) {
                    ans[i] = topk.top().second;
                    i++;
                    topk.pop();
                }
                final_answer[user_num] = ans;
            }
            cout << "User Ended: " << user_num << "\n"; 
        }

        #pragma omp parallel for num_threads(C)
        for(int r = 0; r < (size - 1) * C; r++) {
            vector<double> user(D, INT_MIN);
            vector<int> ans(k, -1);
            MPI_Status status;
            int done = 0;
            MPI_Recv(ans.data(), k, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(ans[0] != -1) {
                int curr_user = rank_user[status.MPI_SOURCE][status.MPI_TAG];
                final_answer[curr_user] = ans;
            }
            MPI_Send(user.data(), D, MPI_DOUBLE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
        }

        for(auto j : final_answer) {
            for(auto val : j.second) fs_output << val << " ";
            fs_output << "\n";
        }
        fs_output.close();

    } else {
        #pragma omp parallel num_threads(C)
        {
            bool done = false;
            MPI_Status status;

            vector<int> ans(k, -1);
            vector<double> user(D, INT_MIN);

            while(!done) {
                MPI_Send(ans.data(), k, MPI_INT, 0, omp_get_thread_num(), MPI_COMM_WORLD);
                MPI_Recv(user.data(), D, MPI_DOUBLE, 0, omp_get_thread_num(), MPI_COMM_WORLD, &status);
                if(user[0] == INT_MIN) {
                    done = true;
                    break;
                }
                priority_queue<pair<double, int>,vector<pair<double,int>>,comp> topk = queryHNSW(user, k, ep, indptr, index, level_offset, max_level, vect);
                int i = 0;
                while(!topk.empty()) {
                    ans[i] = topk.top().second;
                    i++;
                    topk.pop();
                }
            }
        }

    }
    
    MPI_Finalize();

    fs_ep.close(); fs_index.close(); fs_indptr.close(); fs_level_offset.close(); fs_max_level.close(); fs_vect.close();

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    double duration = (1e-6 * (std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)).count());
    if(rank == 0) std::cout << "Time taken for " << size << " processes: " << duration << " ms\n";
    return 0;
}