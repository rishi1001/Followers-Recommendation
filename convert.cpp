#include <bits/stdc++.h>
using namespace std;

int main(int argc, char *argv[]) {
    assert(argc > 2);
    string inDir = string(argv[1]);
    string outDir = string(argv[2]);
    string inFileName, outFileName;
    ifstream inputFile; ofstream outputFile;

    inFileName = inDir + "ep.txt";
    outFileName = outDir + "ep.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    unsigned int ep; inputFile >> ep;
    outputFile.write((char *)&ep, sizeof(unsigned int));
    inputFile.close(); outputFile.close();
    cout << "ep.txt done\n";

    inFileName = inDir + "max_level.txt";
    outFileName = outDir + "max_level.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    unsigned int max_level; inputFile >> max_level;
    outputFile.write((char *)&max_level, sizeof(unsigned int));
    inputFile.close(); outputFile.close();
    cout << "max_level.txt done\n";

    inFileName = inDir + "level.txt";
    outFileName = outDir + "level.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    unsigned int level;
    while(inputFile >> level) {
        outputFile.write((char *)&level, sizeof(unsigned int));
    }
    inputFile.close(); outputFile.close();
    cout << "level.txt done\n";

    inFileName = inDir + "index.txt";
    outFileName = outDir + "index.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    unsigned int index;
    int count = 0;
    while(inputFile >> index) {
        outputFile.write((char *)&index, sizeof(unsigned int));
        count++;
        if(count%10000000 == 0) cout << count << " done\n";
    }
    inputFile.close(); outputFile.close();
    cout << "index.txt done\n";

    inFileName = inDir + "indptr.txt";
    outFileName = outDir + "indptr.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    unsigned int indptr;
    while(inputFile >> indptr) {
        outputFile.write((char *)&indptr, sizeof(unsigned int));
    }
    inputFile.close(); outputFile.close();
    cout << "indptr.txt done\n";

    inFileName = inDir + "level_offset.txt";
    outFileName = outDir + "level_offset.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    unsigned int level_offset;
    while(inputFile >> level_offset) {
        outputFile.write((char *)&level_offset, sizeof(unsigned int));
    }
    inputFile.close(); outputFile.close();
    cout << "level_offset.txt done\n";

    inFileName = inDir + "vect.txt";
    outFileName = outDir + "vect.bin";
    inputFile.open(inFileName.c_str(), ios::in);
    outputFile.open(outFileName.c_str(), ios::out);
    double vect;
    count = 0;
    while(inputFile >> vect) {
        outputFile.write((char *)&vect, sizeof(double));
        count++;
        if(count%10000000 == 0) cout << count << " done\n";
    }
    inputFile.close(); outputFile.close();
    cout << "vect.txt done\n";
}