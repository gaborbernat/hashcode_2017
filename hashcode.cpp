#include <iostream>
#include <fstream>
#include <iterator>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
#include <chrono>
#include <ppl.h>

using namespace std;

struct VEP {
	VEP() {}

	VEP(unsigned short int _v, unsigned short int _e, unsigned int _pop) : v(_v), e(_e), p(_pop) {

	}

	unsigned short int v;
	unsigned short int e;
	unsigned int p;
};

class VideoDistribute {
public:
	VideoDistribute(string filename) {
		ifstream input(filename);
		if (input.is_open()) {
			input >> V >> E >> R >> C >> X;

			istream_iterator<int> read_it(input);
			copy_n(read_it, V, std::back_inserter(video_sizes));
			dc_latencies.resize(E);

			for (unsigned short int e = 0; e < E; ++e) {
				unsigned short int dc_latency, connected_count;
				input >> dc_latency >> connected_count;
				dc_latencies[e] = dc_latency;
				for (int k = 0; k < connected_count; ++k) {
					int cache_id, latency_cache;
					input >> cache_id >> latency_cache;
					endpoint_to_cache_to_latency[e].insert(make_pair(cache_id, latency_cache));
				}
			}
			video_endpoint_popularity.resize(R);
			for (unsigned short int r = 0; r < R; ++r) {
				unsigned short v, e;
				unsigned int p;
				input >> v >> e >> p;
				VEP &vep = video_endpoint_popularity[r];
				vep.e = e;
				vep.v = v;
				vep.p = p;
			}
			cout << "read done" << endl;
		}
		else {
			throw runtime_error("cannot open " + filename);
		}
	}

	unsigned long long score() {
		using namespace Concurrency;
		combinable<uint64_t> part_sums([] { return 0; });
		combinable<uint64_t> part_rs([] { return 0; });

		concurrency::parallel_for_each(begin(video_endpoint_popularity),
			end(video_endpoint_popularity),
			[this, &part_sums, &part_rs](const VEP& vep) {
			unsigned short int to_dc = dc_latencies[vep.e];
			unsigned short int min_latency = to_dc;
			for (auto c_to_l : endpoint_to_cache_to_latency[vep.e]) {
				auto c = c_to_l.first;
				auto videos_in_cache = cache_to_videos[c];
				if (videos_in_cache.find(vep.v) != videos_in_cache.end()) {
					if (min_latency > c_to_l.second) {
						min_latency = c_to_l.second;
					}
				}
			}
			part_sums.local() += (to_dc - min_latency) * vep.p;
			part_rs.local() += vep.p;
		});
		return part_sums.combine(std::plus<uint64_t>()) * 1000 / part_rs.combine(std::plus<uint64_t>());
	}

	unsigned long long run() {
		sort(begin(video_endpoint_popularity), end(video_endpoint_popularity), [](auto &f, auto &s) {
			return f.p > s.p;
		});
		cout << "sorted" << endl;
		cache_to_videos = vector<set<unsigned short >>(C);
		cache_size = vector<unsigned int>(C);
		for (auto vep : video_endpoint_popularity) {

			auto i = endpoint_to_cache_to_latency[vep.e];
			pair<unsigned short int, unsigned short> min = make_pair<unsigned short int, unsigned short int>(INT16_MAX,
				INT16_MAX);
			for (auto c_l : endpoint_to_cache_to_latency[vep.e]) {
				if (video_sizes[vep.v] + cache_size[c_l.first] <= X) {
					if (min.first == INT16_MAX || min.second < c_l.second) {
						min.first = c_l.first;
						min.second = c_l.second;
					}
				}
			}
			if (min.first != INT16_MAX) {
				cache_size[min.first] += video_sizes[vep.v];
				cache_to_videos[min.first].insert(vep.v);
			}
		}
		cout << "selected" << endl;
		return score();
	}

	void write(string filename) {
		ofstream f(filename);
		f << cache_to_videos.size() << endl;
		unsigned short int i = 0;
		for (auto a : cache_to_videos) {
			f << i++ << " ";
			std::copy(a.begin(), a.end(), std::ostream_iterator<int>(f, " "));
			f << endl;
		}
	}

private:
	unsigned int V, E, R, C, X;
	vector<unsigned int> video_sizes;
	vector<unsigned short int> dc_latencies;
	map<unsigned short int, map<unsigned short int, unsigned short int>> endpoint_to_cache_to_latency;
	vector<VEP> video_endpoint_popularity;

	vector<set<unsigned short int>> cache_to_videos;
	vector<unsigned int> cache_size;

};


int main() {
	string dir = "C:\\Users\\berna\\ClionProjects\\video_distribute\\";
	unsigned long long sum = 0;
	for (auto s : { "kittens", "videos_worth_spreading", "trending_today", "kittens" }) {
		auto start = chrono::steady_clock::now(); //use a
		VideoDistribute v(dir + "in\\" + s + ".in");
		unsigned long long i = v.run();
		sum += i;
		cout << s << " " << i << endl;
		v.write(dir + s + ".out");
		auto end = chrono::steady_clock::now();
		auto diff = end - start;
		cout << "Elapsed time is :  " << chrono::duration_cast<chrono::milliseconds>(diff).count() << " ms " << endl;
	}
	cout << sum << endl;
	return 0;
}
