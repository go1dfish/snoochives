var RSVP = require('rsvp');
var _ = require('underscore');
var express = require('express');
var loki = require('lokijs');
var snoosnort = require('snoosnort');
var events = require('events');
var fs = require('fs');

var unsafeLoadJSON = loki.prototype.loadJSON
loki.prototype.loadJSON = function(data) {
  try {
    return unsafeLoadJSON.apply(this, arguments);
  } catch(e) {
    console.error('Warning unable to load JSON', data);
  }
};

var mkdirSync = function (path) {
  try {
    fs.mkdirSync(path);
  } catch(e) {
    if ( e.code != 'EEXIST' ) throw e;
  }
}

String.prototype.rsplit = function(sep, maxsplit) {
    var split = this.split(sep);
    return maxsplit ? [ split.slice(0, -maxsplit).join(sep) ].concat(split.slice(-maxsplit)) : split;
};

module.exports = function(reddit, path, types, schedule) {
  var path = path || 'ingest/';
  var emitter = new events.EventEmitter();
  var locks = {};
  var snort = snoosnort(reddit, types || {
    t1: {depth: 10000, extra: []},
    t3: {depth: 1000, extra: []}
  }, schedule);

  mkdirSync(path);

  snort.on('t1', archive); snort.on('t3', archive);

  _.keys(types).forEach(function(type) {snort.on(type, archive)});

  emitter.promise = snort.promise;
  emitter.snort = snort;
  emitter.read = getLock;
  emitter.edit = edit;
  emitter.getPostIds = getPostIds;
  emitter.getSubs = getSubs;
  snort.on('t1', function(item) {
    archive({
      id: item.link_id.split('_').pop(),
      name: 't3_' + item.link_id.split('_').pop(),
      subreddit: item.subreddit,
      url: item.link_url
    });
  });
  return emitter;

  function archiveFormat(item, extra) {var data = {}; extra = extra || [];
    ['id'].concat(extra).forEach(function(j) {data[j] = item[j];});
    return data;
  }

  function getDb(name) {name = name.toLowerCase();
    var filePath = path + '/' + name;
    var parts = filePath.split('/');
    parts.pop();
    mkdirSync(parts.join('/'));
    return new RSVP.Promise(function(resolve, reject) {
      try {
        var db = new loki(filePath + '.json', {
          autoload: true,
          autoloadCallback: function(e) {
            resolve(db);
          }
        });
      } catch(e) {reject(e);}
    });
  }

  function getSubreddit(subreddit) {
    return getDb(subreddit).then(function(db) {
      db.t3 = db.getCollection('t3') || db.addCollection('t3');
      return db;
    });
  }

  function getLink(subreddit, id) {
    if (!id) {return getSubreddit(subreddit);}
    return getDb(subreddit + '/' + id).then(function(db) {
      db.t1 = db.getCollection('t1') || db.addCollection('t1');
      return db;
    });
  }

  function getLock(subreddit, id, doStuff) {
    var name =_.compact([subreddit, id]).join('/');
    var lock = locks[name];
    if (!doStuff) {doStuff = function(db) {return db;}}
    if (lock) {return lock.then(function() {
      return getLock(subreddit, id, doStuff);
    })}
    return locks[name] = getLink(subreddit, id).then(function(db) {
      return doStuff(db);
    }).finally(function() {delete locks[name];})
  }

  function edit(subreddit, id, editCb) {
    return getLock(subreddit, id, function(db) {
      function save() {db.saveDatabase();}
      var signals = ['SIGINT', 'SIGTERM', 'SIGHUP'];
      signals.forEach(function(j) {process.on(j, save);});
      return RSVP.resolve(editCb(db)).then(function() {
        return new RSVP.Promise(function(resolve, reject) {
          db.saveDatabase(function(e) {if (e) {reject(e);} else {resolve(db);}});
        });
      }).catch(function(e) {
        console.error(e.stack || e);
        throw e;
      }).finally(function(res) {
        signals.forEach(function(j) {process.removeListener(j, save);});
        return res;
      });
    });
  }

  function archive(item, opts) {opts = opts || {};
    if (!item || !item.subreddit || !item.id) {return RSVP.resolve();}
    function addToDb(db) {var type = item.name.split('_')[0];
      try {
        if (db[type].find({id:item.id}).length) {return;}
        var data = archiveFormat(item, opts.extra);
        db[type].insert(data);
        emitter.emit(type, item);
      } catch(e) {console.error(e.stack || e);}
    }
    return edit(item.subreddit, (item.link_id||'').split('_').pop(), addToDb);
  }

  function getFileNames(dirPath) {
    return new RSVP.Promise(function(resolve, reject) {
      fs.readdir(dirPath, function(e, names) {
        if (e) {return reject(e);}
        resolve(names);
      });
    }).catch(function(e) {return [];});
  }

  function getFileIds(dirPath) {
    return getFileNames(dirPath).then(function(names) {
      return names.filter(function(j){return j.match(/\.json/);})
        .map(function(j) {return j.split('.')[0];});
    }).catch(function(e) {return [];});
  }

  function getPostIds(subreddit) {
    return getFileIds(path+'/'+subreddit.toLowerCase());
  }

  function getSubs() {return getFileIds(path);}
}
