#include <iostream>
#include<fstream>
#include<unordered_map>
#include<string>
#include<vector>
#include<algorithm>
#include<queue>

static constexpr char* ConfigPath = "../data/config.ini";
static constexpr char* DemandPath = "../data/demand.csv";
static constexpr char* QosPath = "../data/qos.csv";
static constexpr char* SiteBandwidthPath = "../data/site_bandwidth.csv";
static constexpr char* OUTPATH = "../output/solution.txt";
static constexpr float PERCENT = 0.95;

class FileReader {
public:
	FileReader() :qosConstraint(0) {};
public:
	bool createGraph(const std::string& configPath, const std::string& demandPath, const std::string& qosPath, const std::string& siteBWPath) {
		if (!getQosConstraint(configPath) || !getDemands(demandPath) || !getQos(qosPath) || !getServerSites(siteBWPath)) {
			return false;
		}
		if (outfile.is_open()) outfile.close();
		outfile.open(OUTPATH, std::ios::out | std::ios::trunc);
		outfile.close();
		return true;
	}

	void outputRet(const std::string& outputPath, std::vector<std::unordered_map<int, int>>& momentRet, bool isEOF) {
		if (outfile.is_open()) outfile.close();
		outfile.open(outputPath, std::ios::out | std::ios::app);
		int n = momentRet.size();
		for (int i = 0; i < n; ++i) {
			std::string cliName = clientName[i];
			outfile << cliName << ':';
			int j = 0;
			int m = momentRet[i].size();
			for (auto& info : momentRet[i]) {
				std::string serName = serverName[info.first];
				int bandwidth = info.second;
				outfile << '<' << serName << ',' << bandwidth << '>';
				if (++j < m) {
					outfile << ',';
				}
			}
			if (isEOF && i == n - 1) {
				continue;
			}
			else outfile << '\n';
		}
		outfile.close();
	}
private:
	bool getDemands(const std::string& path) {
		if (infile.is_open()) infile.close();
		infile.open(path, std::ios::in);
		if (!infile) {
			std::cout << "failed" << std::endl;
			return false;
		}
		std::string line;
		int clientCount = 0;
		int i = -1;
		while (std::getline(infile, line)) {
			if (i == -1) {
				size_t pos = 0;
				size_t prev = 0;
				if ((pos = line.find(',', prev)) != std::string::npos) {
					prev = pos + 1;
				}
				else return false;
				while ((pos = line.find(',', prev)) != std::string::npos) {
					prev = pos + 1;
					clientCount++;
				}
				clientCount++;
			}
			else {
				size_t pos = 0;
				size_t prev = 0;
				if ((pos = line.find(',', prev)) != std::string::npos) {
					prev = pos + 1;
				}
				else return false;
				int j = 0;
				allDemands.emplace_back(clientCount, 0);
				while ((pos = line.find(',', prev)) != std::string::npos) {
					std::string dem = line.substr(prev, pos - prev);
					allDemands[i][j] = stoi(dem);
					++j;
					prev = pos + 1;
				}
				std::string dem = line.substr(prev);
				allDemands[i][j] = stoi(dem);
				++j;
			}
			++i;
		}
		infile.close();
		return true;
	}

	bool getQos(const std::string& path) {
		if (infile.is_open()) infile.close();
		infile.open(path, std::ios::in);
		if (!infile) {
			std::cout << "failed" << std::endl;
			return false;
		}
		std::string line;
		int clientCount = 0;
		int i = -1;
		while (std::getline(infile, line)) {
			if (i == -1) {
				size_t pos = 0;
				size_t prev = 0;
				if ((pos = line.find(',', prev)) != std::string::npos) {
					prev = pos + 1;
				}
				else return false;
				while ((pos = line.find(',',prev)) != std::string::npos) {
					std::string client = line.substr(prev, pos - prev);
					clientName.emplace_back(client);
					clientId[client] = clientCount++;
					prev = pos + 1;
				}
				std::string client = line.substr(prev);
				clientName.emplace_back(client);
				clientId[client] = clientCount++;
				clientEdges.assign(clientCount, std::vector<int>());
			}
			else {
				size_t pos = 0;
				size_t prev = 0;
				if ((pos = line.find(',', prev)) != std::string::npos) {
					std::string server = line.substr(prev, pos - prev);
					serverName.emplace_back(server);
					serverId[server] = i;
					prev = pos + 1;
				}
				else return false;
				int j = 0;
				allQos.emplace_back(clientCount,0);
				serverEdges.emplace_back();
				while ((pos = line.find(',', prev)) != std::string::npos) {
					std::string qos = line.substr(prev, pos - prev);
					int num = stoi(qos);
					if (num < qosConstraint) {
						clientEdges[j].emplace_back(i);
						serverEdges[i].emplace_back(j);
					}
					allQos[i][j] = num;
					++j;
					prev = pos + 1;
				}
				std::string qos = line.substr(prev);
				int num = stoi(qos);
				if (num < qosConstraint) {
					clientEdges[j].emplace_back(i);
					serverEdges[i].emplace_back(j);
				}
				allQos[i][j] = stoi(qos);
				++j;
			}
			++i;
		}
		infile.close();
		return true;
	}

	bool getServerSites(const std::string& path) {
		if (infile.is_open()) infile.close();
		infile.open(path, std::ios::in);
		if (!infile) {
			std::cout << "failed" << std::endl;
			return false;
		}
		std::string line;
		int i = 0;
		while (std::getline(infile, line)) {
			if (i == 0) {
				++i;
				continue;
			}
			size_t pos = 0;
			if ((pos = line.find(',')) != std::string::npos) {
				std::string name = line.substr(0, pos);
				int bandwidth = std::stoi(line.substr(pos + 1));
				serverSites[name] = bandwidth;
			}
		}
		infile.close();
		return true;
	}

	bool getQosConstraint(const std::string& path) {
		if (infile.is_open()) infile.close();
		infile.open(path, std::ios::in);
		if (!infile) {
			std::cout << "failed" << std::endl;
			return false;
		}
		std::string line;
		int i = 0;
		std::string target = "qos_constraint";
		int pos = 0;
		while (std::getline(infile, line)) {
			if ((pos = line.find(target)) != std::string::npos) {
				pos += target.size() + 1;
				qosConstraint = stoi(line.substr(pos));
				return true;
			}
		}
		return false;
	}
public:
	std::vector<std::vector<int>> clientEdges;	//各客户节点的可用边缘节点
	std::vector<std::vector<int>> serverEdges;	//各边缘节点的可用客户节点
	std::vector<std::vector<int>> allDemands;	//各时刻带宽需求
	std::unordered_map<std::string, int> serverSites; //边缘节点带宽
	int qosConstraint;	//时延限制
	std::vector<std::vector<int>> allQos;	//时延
	std::unordered_map<std::string, int> clientId;	//客户节点ID
	std::unordered_map<std::string, int> serverId;	//边缘节点ID
	std::vector<std::string> clientName;	//客户节点name
	std::vector<std::string> serverName;	//边缘节点name
private:
	std::ifstream infile;
	std::ofstream outfile;
};

enum class SortFlag{
	LESS=0,
	GREATER=1
};

class Allocation {
	
public:
	bool init(const std::string& configPath, const std::string& demandPath, const std::string& qosPath, const std::string& siteBWPath) {
		if (!fileread->createGraph(configPath, demandPath, qosPath, siteBWPath)) {
			return false;
		}
		mySort(fileread->clientEdges, fileread->serverEdges, SortFlag::GREATER);
		return true;
	}

	void allocate() {
		int serverNum = (int)fileread->serverId.size();
		int demandsNum = (int)fileread->allDemands.size();
		maxPeak = (int)demandsNum * (1 - PERCENT);
		peakNum.assign(serverNum, 0);
		for (int i = 0; i < demandsNum; ++i) {
			momentRet.assign(fileread->allDemands[i].size(), std::unordered_map<int, int>());
			process(fileread->allDemands[i]);
			bool isEOF = i == demandsNum - 1 ? true : false;
			fileread->outputRet(OUTPATH, momentRet,isEOF);
		}
	}

	void process(std::vector<int>& demand) {
		int n = demand.size();
		std::vector<int> visited(n, 0);
		std::vector<int> bandRemain(fileread->serverSites.size(), 0);
		for (int i = 0; i < bandRemain.size(); ++i) {
			bandRemain[i] = fileread->serverSites[fileread->serverName[i]];
		}
		for (int i = 0; i < n; ++i) {
			if (demand[i] == 0) continue;
			satisfyClient(i, demand, visited, bandRemain);
		}
	}

	void satisfyClient(int i, std::vector<int>& demand, std::vector<int>& visited, std::vector<int>& bandRemain) {
		int m = fileread->clientEdges[i].size();
		for (int j = 0; j < m; ++j) {
			if (demand[i] == 0) return;
			int serverId = fileread->clientEdges[i][j];
			if (peakNum[serverId] >= maxPeak || bandRemain[serverId] <=0) {
				continue;
			}
			for (auto& clientId : fileread->serverEdges[serverId]) {
				if (bandRemain[serverId] <= 0) {
					break;
				}
				if (demand[clientId] == 0) {
					continue;
				}
				if (demand[clientId] < bandRemain[serverId]) {
					momentRet[clientId][serverId] += demand[clientId];
					bandRemain[serverId] -= demand[clientId];
					demand[clientId] = 0;
				}
				else {
					momentRet[clientId][serverId] += bandRemain[serverId];
					demand[clientId] -= bandRemain[serverId];
					bandRemain[serverId] = 0;
					break;
				}
			}
			++peakNum[serverId];
		}
		//所有边缘节点均已按5%最大分配，但客户节点需求未满足,接下来采用 平均分配策略
		while (demand[i] > 0) {
			int serverNum = fileread->clientEdges[i].size();
			int averageDemand = demand[i] / serverNum;
			int residual = demand[i] % serverNum;
			for (auto& serverId : fileread->clientEdges[i]) {
				if (bandRemain[serverId] > 0) {
					if (bandRemain[serverId] >= averageDemand) {
						momentRet[i][serverId] += averageDemand;
						demand[i] -= averageDemand;
						bandRemain[serverId] -= averageDemand;
						if (residual > 0 && bandRemain[serverId] > 0) {
							if (bandRemain[serverId] >= residual) {
								momentRet[i][serverId] += residual;
								demand[i] -= residual;
								bandRemain[serverId] -= residual;
								residual = 0;
							}
							else {
								momentRet[i][serverId] += bandRemain[serverId];
								demand[i] -= bandRemain[serverId];
								residual -= bandRemain[serverId];
								bandRemain[serverId] = 0;
							}
						}
					}
					else {
						momentRet[i][serverId] += bandRemain[serverId];
						demand[i] -= bandRemain[serverId];
						bandRemain[serverId] = 0;
					}
				}
			}
		}
	}

	void mySort(std::vector<std::vector<int>>& Edges1, std::vector<std::vector<int>> Edges2, SortFlag flag= SortFlag::GREATER) {
		if (flag == SortFlag::GREATER) {
			for (auto& it : Edges1) {
				std::sort(it.begin(), it.end(), [&Edges2](const int& a, const int& b) {
					return Edges2[a].size() > Edges2[b].size();
					});
			}
		}
		else if (flag == SortFlag::LESS) {
			for (auto& it : Edges1) {
				std::sort(it.begin(), it.end(), [&Edges2](const int& a, const int& b) {
					return Edges2[a].size() < Edges2[b].size();
					});
			}
		}
	}
public:
	Allocation() :fileread(new FileReader), maxPeak(0) {}
	~Allocation() {
		if (fileread != nullptr) {
			delete fileread;
			fileread = nullptr;
		}
	}
private:
	std::vector<int> peakNum;	//各边缘节点峰值次数
	int maxPeak;
private:
	FileReader* fileread;
private:
	std::vector<std::unordered_map<int, int>> momentRet;	//单个时刻的分配方案
};

int main() {
	Allocation allocation;
	allocation.init(ConfigPath, DemandPath, QosPath, SiteBandwidthPath);
	allocation.allocate();
	return 0;
}