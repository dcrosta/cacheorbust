#ifndef KTCACHEORBUST_H
#define KTCACHEORBUST_H

#define CACHE_OR_BUST_VERSION "0.1.0"

#include <ktplugserv.h>
#include "fetch.h"

namespace kc = kyotocabinet;
namespace kt = kyototycoon;

namespace cob {
  class CacheOrBust : public kt::PluggableServer {
    private:
      class Worker;

      // borrowed
      kt::TimedDB* _db;

      // owned
      kt::ThreadedServer _serv;
      Worker* _worker;

      cob::FetchQueue* _queue;

      double _stime;

      std::string _host;
      int32_t _port;
      uint32_t _server_threads;
      uint32_t _fetcher_threads;
      uint32_t _ttl;

    public:
      explicit CacheOrBust() :
          _stime(kc::time()),
          _db(NULL),
          _worker(NULL),
          _queue(NULL),
          _host(""),
          _port(0),
          _server_threads(0),
          _fetcher_threads(0),
          _ttl(0),
          _serv()
      {};

      void configure(kt::TimedDB* dbary, size_t dbnum,
          kt::ThreadedServer::Logger* logger, uint32_t logkinds,
          const char* expr);

      bool start();
      bool stop();
      bool finish();

      void log(kt::ThreadedServer::Logger::Kind kind, const char* format, ...)
      {
        va_list ap;
        _serv.log(kind, format, ap);
      }

      void log(kt::ThreadedServer::Logger::Kind kind, std::string message)
      {
        _serv.log(kind, message.c_str());
      }

    private:
      class Worker : public kt::ThreadedServer::Worker {
        // opcounters
        enum {
          HIT,
          MISS,
          ENQUEUE,
          FETCH,
          FLUSH,
          LAST_OP_
        };
        typedef uint64_t OpCounts[LAST_OP_];

        private:
        // borrowed
        CacheOrBust* _serv;

        // owned
        OpCounts* _opcounts;

        int32_t _nthreads;

        public:
          explicit Worker(CacheOrBust* serv, int32_t nthreads) :
            _serv(serv), _nthreads(nthreads), _opcounts(NULL)
          {
            _opcounts = new OpCounts[_nthreads];
            for (int32_t i = 0; i < _nthreads; i++) {
              for (int32_t j = 0; j < LAST_OP_; j++) {
                _opcounts[i][j] = 0;
              }
            }
          };

          ~Worker() {
            delete _opcounts;
          };

          bool process(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess);

          bool do_get(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
              const std::vector<std::string>& tokens, kt::TimedDB* db);
          bool do_flush(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
              const std::vector<std::string>& tokens, kt::TimedDB* db);
          bool do_stats(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess,
              const std::vector<std::string>& tokens, kt::TimedDB* db);
      };
  };
};

#endif
// vim:filetype=cpp ts=2 sts=2 sw=2 expandtab
