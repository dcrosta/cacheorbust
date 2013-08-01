#include "ktcacheorbust.h"
#include "fetch.h"

using namespace cob;

void FetchQueue::do_task(kc::TaskQueue::Task* t)
{
  FetchTask* task = static_cast<FetchTask*>(t);

  std::string& rawurl(task->_url);
  int32_t ttl(task->_ttl);

  kt::URL url(rawurl);
  if (url.host().empty() || 0 == url.port()) {
    std::stringstream msg;
    msg << "illegal URL '" << rawurl << "'";
    _serv->log(kt::ThreadedServer::Logger::INFO, msg.str());
    _db->remove(rawurl);
    delete task;
    return;
  }

  std::stringstream msg;
  msg << "fetching '" << rawurl << "'";
  _serv->log(kt::ThreadedServer::Logger::DEBUG, msg.str());

  std::string response;
  kt::HTTPClient* client(get_client(url));
  client->open(url.host(), url.port(), 5);
  if (-1 == client->fetch(url.path_query(), kt::HTTPClient::MGET, &response)) {
    std::stringstream msg;
    msg << "failed to fetch URL '" << rawurl << "': " << response;
    _serv->log(kt::ThreadedServer::Logger::ERROR, msg.str());
    _db->remove(rawurl);
    return_client(url, client, false);
    delete task;
    return;
  }
  return_client(url, client);

  std::stringstream record;
  record << '\0';
  record << response;

  if (!_db->set(rawurl, record.str(), ttl)) {
    _db->remove(rawurl);
  }

  delete task;
}

kt::HTTPClient* FetchQueue::get_client(kt::URL& url)
{
  kc::ScopedMutex lk(&_lock);

  // try to find an unused client for the host/port
  std::stringstream keystream;
  keystream << url.host() << ":" << url.port();
  std::string key(keystream.str());

  std::pair<clientiter, clientiter> iters(_clients.equal_range(key));
  while (iters.first != iters.second) {
    entry& elem(iters.first->second);
    if (!elem.second) {
      // found one
      elem.second = true;
      return elem.first;
    }
    ++iters.first;
  }

  // if we are at the client pool capacity, find
  // one unused to delete. since the pool size is
  // double the number of threads, there should be
  // unused clients that can be deleted.
  while (_clients.size() >= _nthreads * 2) {
    clientiter it(_clients.begin());
    clientiter end(_clients.end());
    while (it != end) {
      entry& elem(it->second);
      if (!elem.second) {
        elem.first->close();
        delete elem.first;
        _clients.erase(it);
      }
      ++it;
    }
  }
  _assert_(_clients.size() < _nthreads * 2);

  kt::HTTPClient* client = new kt::HTTPClient;
  client->open(url.host(), url.port(), 5);

  entry val(client, true);
  std::pair<std::string, entry> item(key, val);
  _clients.insert(item);

  return client;
}

void FetchQueue::return_client(kt::URL& url, kt::HTTPClient* client, bool keep)
{
  kc::ScopedMutex lk(&_lock);

  std::stringstream keystream;
  keystream << url.host() << ":" << url.port();
  std::string key(keystream.str());

  // mark the client as no longer in use
  std::pair<clientiter, clientiter> iters(_clients.equal_range(key));
  while (iters.first != iters.second) {
    entry& elem(iters.first->second);
    if (elem.first == client) {
      if (keep) {
        elem.second = false;
        return;
      } else {
        _clients.erase(iters.first);
        return;
      }
    }
    ++iters.first;
  }

  // could not find client in the pool, clean up
  client->close();
  delete client;
}

// vim:filetype=cpp ts=2 sts=2 sw=2 expandtab
