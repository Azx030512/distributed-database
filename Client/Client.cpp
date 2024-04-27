#include<iostream>
#include<curl/curl.h>
#include<fstream>

using namespace std;

int main() {
	string zoo_keeper_host = "127.0.0.1:2181";
	string table_name;
	cin >> table_name;

	CURL* curl;
	curl = curl_easy_init();
	CURLcode res = get_region_server_ip(zoo_keeper_host, table_name);
	string ip;
	string port;
	string file_path;
	if (res == CURLE_OK) {
		curl_easy_getinfo(curl, CURLINFO_PRIMARY_IP, ip);
		curl_easy_getinfo(curl, CURLINFO_PRIMARY_PORT, port);
		curl_easy_getinfo(curl, CURLINFO_REDIRECT_URL, file_path);
	}
	else {
		cout << "Error: Failed to get region server information from Zookeeper." << endl;
		return 1;
	}

	res = get_rpc_file(ip, port, file_path);
	if (res == CURLE_OK) {
		ofstream file("rpc_file.txt", ios::binary);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &file);
		res = curl_easy_perform(curl);
		file.close();
	}
	else {
		cout << "Error: Failed to get file from RegionServer." << endl;
		return 1;
	}

	curl_easy_cleanup(curl);
	return 0;
}

CURLcode get_region_server_ip(string zookeeper_host, string table_name) {
	CURL* curl;
	CURLcode res;
	curl = curl_easy_init();
	if (curl) {
		string url = "http://" + zookeeper_host + "/get_region_server_ip";
		string params = "table_name=" + table_name;

		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, params.c_str());

		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);
	}

	return res;
}

CURLcode get_rpc_file(string region_server_ip, string region_server_port, string file_path) {
	CURL* curl;
	CURLcode res;
	curl = curl_easy_init();
	if (curl) {
		string url = "http://" + region_server_ip + ":" + region_server_port + "/" + file_path;
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

		res = curl_easy_perform(curl);
		curl_easy_cleanup(curl);
	}

	return res;
}