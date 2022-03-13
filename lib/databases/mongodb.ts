import async from 'async';
import stream from 'stream';
import util from 'util';
import debugLib from 'debug';

import _ from 'lodash';
import {
  Collection,
  Filter,
  FindOneAndUpdateOptions,
  MongoClient,
  MongoClientOptions,
  ObjectId,
  UpdateFilter,
} from 'mongodb';

import Store from '../base';

const debug = debugLib('eventstore:store:mongodb');

interface EventDoc {
  _id: ObjectId;
  streamId: string;
  aggregateId: string;
  aggregate: string;
  context?: unknown;
  streamRevision: number;
  commitId: string;
  commitSequence: number;
  commitStamp: Date;
  payload: Record<string, any>;
  position: number;
  id: ObjectId;
  restInCommitStream: number;
  dispatched: boolean;
}

interface PositionDoc {
  _id: string;
  position: number;
}

interface SnapshotDoc {
  _id: string;
}

interface TransactionDoc {
  _id: string;
  aggregateId: string;
  aggregate: Object;
  context: Object;
  events: EventDoc[];
}

class MongoClass {
  private client: MongoClient;

  private maxSnapshotsCount: number;
  private positionsCounterId: string;

  private events: Collection<EventDoc>;
  private positions: Collection<PositionDoc>;
  private snapshots: Collection<SnapshotDoc>;
  private transactions: Collection<TransactionDoc>;

  constructor({
    dbName = 'eventstore',
    eventsCollectionName = 'events',
    host = 'localhost',
    maxSnapshotsCount,
    port = 27017,
    positionsCollectionName = 'positions',
    snapshotsCollectionName = 'snapshots',
    transactionsCollectionName = 'transactions',
  }: {
    dbName: string,
    eventsCollectionName: string,
    host: string,
    maxSnapshotsCount?: number,
    port: number,
    positionsCollectionName: string,
    snapshotsCollectionName: string,
    transactionsCollectionName: string
  }, options?: MongoClientOptions) {
    const url = `mongodb://${host}:${port}/${dbName}`;
    this.maxSnapshotsCount = maxSnapshotsCount;
    this.initializeClient(url, options, { eventsCollectionName, positionsCollectionName, snapshotsCollectionName, transactionsCollectionName });
  }

  public addEvents = (events: Array<EventDoc>, callback) => {
    if (events.length === 0) {
      if (callback) { callback(null); }
      return;
    }

    const { commitId } = events[0];

    let noAggregateId = false;
    let invalidCommitId = false;

    events.forEach((evt) => {
      if (!evt.aggregateId) {
        noAggregateId = true;
      }

      if (!evt.commitId || evt.commitId !== commitId) {
        invalidCommitId = true;
      }

      evt._id = evt.id;
      evt.dispatched = false;
    });

    if (noAggregateId) {
      const errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    if (invalidCommitId) {
      const errMsg = 'commitId not defined or different!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    if (events.length === 1) {
      return this.events.insertOne(events[0], callback);
    }

    var tx: TransactionDoc = {
      _id: commitId,
      events: events,
      aggregateId: events[0].aggregateId,
      aggregate: events[0].aggregate,
      context: events[0].context
    };

    this.transactions.insertOne(tx, function (err) {
      if (err) {
        debug(err);
        if (callback) callback(err);
        return;
      }

      this.events.insertMany(events, function (err) {
        if (err) {
          debug(err);
          if (callback) callback(err);
          return;
        }

        this.removeTransactions(events[events.length - 1], callback);
      });
    });
  };

  public addSnapshot = (snap, callback) => {
    console.log('Add snapshot called...', { snap });
    if (!snap.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    snap._id = snap.id;
    this.snapshots.insertOne(snap, callback);
  }

  public cleanSnapshots = (query, callback) => {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement: Filter<SnapshotDoc> = {
      aggregateId: query.aggregateId
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    this.snapshots.find(findStatement, {
      sort: [['revision', 'desc'], ['version', 'desc'], ['commitStamp', 'desc']]
    })
      .skip(this.maxSnapshotsCount)
      .toArray(removeElements(this.snapshots, callback));
  }

  public clear = async () => {
    await Promise.all([
      this.events.deleteMany({}),
      this.positions.deleteMany({}),
      this.snapshots.deleteMany({}),
      this.transactions.deleteMany({})
    ]);
  }

  public connect = async (callback: (...args: any[]) => void) => {
    await this.client.connect();
    if (callback) callback();
  };

  public disconnect = async (...args) => {
    console.log('Called disconnect', ...args);
  };

  public getUndispatchedEvents = (query, callback) => {
    var findStatement: Filter<EventDoc> = {
      dispatched: false
    };

    if (query && query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query && query.context) {
      findStatement.context = query.context;
    }

    if (query && query.aggregateId) {
      findStatement.aggregateId = query.aggregateId;
    }

    return this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] }).toArray(callback);
  }
  
  public getEvents = (query, skip, limit, callback) => {
    this.streamEvents(query, skip, limit).toArray(callback);
  }

  public getEventsByRevision = (query, revMin, revMax, callback) => {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    const streamRevOptions: Filter<EventDoc>['streamRevision'] = { '$gte': revMin, '$lt': revMax };
    if (revMax === -1) {
      streamRevOptions.$gte = revMin;
    }

    const findStatement: Filter<EventDoc> = {
      aggregateId: query.aggregateId,
      streamRevision: streamRevOptions
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] }).toArray(function (err, res) {
      if (err) {
        debug(err);
        return callback(err);
      }

      if (!res || res.length === 0) {
        return callback(null, []);
      }

      var lastEvt = res[res.length - 1];

      var txOk = (revMax === -1 && !lastEvt.restInCommitStream) ||
                 (revMax !== -1 && (lastEvt.streamRevision === revMax - 1 || !lastEvt.restInCommitStream));

      if (txOk) {
        // the following is usually unnecessary
        this.removeTransactions(lastEvt);

        return callback(null, res);
      }

      this.repairFailedTransaction(lastEvt, function (err) {
        if (err) {
          if (err.message.indexOf('missing tx entry') >= 0) {
            return callback(null, res);
          }
          debug(err);
          return callback(err);
        }

        this.getEventsByRevision(query, revMin, revMax, callback);
      });
    });
  }
  
  public getEventsSince = (date, skip, limit, callback) => {
    this.streamEventsSince(date, skip, limit).toArray(callback);
  }

  public getLastEvent = (query, callback) => {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    const findStatement: Filter<EventDoc> = { aggregateId: query.aggregateId };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    this.events.findOne(findStatement, { sort: [['commitStamp', 'desc'], ['streamRevision', 'desc'], ['commitSequence', 'desc']] }, callback);
  };

  public getNewId = (callback: (any, string) => void, ...rest) => {
    console.log('Called getNewId', ...rest);
    callback(null, new ObjectId().toString());
  };

  public getNextPositions = (positions, callback) => {
    if (!this.positions)
      return callback(null);

    const query: Filter<PositionDoc> = { _id: this.positionsCounterId };
    const update: UpdateFilter<PositionDoc> = { $inc: { position: positions } };
    const options: FindOneAndUpdateOptions = { returnDocument: 'after', upsert: true };

    this.positions.findOneAndUpdate(query, update, options, (err, pos) => {
      if (err)
        return callback(err);

      pos.value.position += 1;

      callback(null, _.range(pos.value.position - positions, pos.value.position));
    });
  };

  public getPendingTransactions = (callback) => {
    this.transactions.find({}).toArray(function (err, txs) {
      if (err) {
        debug(err);
        return callback(err);
      }

      if (txs.length === 0) {
        return callback(null, txs);
      }

      var goodTxs = [];

      async.map(txs, function (tx, clb) {
        var findStatement: Filter<EventDoc> = { commitId: tx._id, aggregateId: tx.aggregateId };

        if (tx.aggregate) {
          findStatement.aggregate = tx.aggregate;
        }

        if (tx.context) {
          findStatement.context = tx.context;
        }

        this.events.findOne(findStatement, function (err, evt) {
          if (err) {
            return clb(err);
          }

          if (evt) {
            goodTxs.push(evt);
            return clb(null);
          }
          
          this.transactions.deleteOne({ _id: tx._id }, clb);
        });
      }, function (err) {
        if (err) {
          debug(err);
          return callback(err);
        }

        callback(null, goodTxs);
      })
    });
  };

  public getSnapshot = (query, revMax, callback) => {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement: Filter<SnapshotDoc> = {
      aggregateId: query.aggregateId
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    if (revMax > -1) {
      findStatement.revision = { '$lte': revMax };
    }

    this.snapshots.findOne(findStatement, { sort: [['revision', 'desc'], ['version', 'desc'], ['commitStamp', 'desc']] }, callback);
  };

  private initializeClient = async (url: string, options: MongoClientOptions, {
    eventsCollectionName, positionsCollectionName, snapshotsCollectionName, transactionsCollectionName
  }) => {
    
    const client = new MongoClient(url, {
      ...options,
    });
    this.client = client;

    this.positionsCounterId = eventsCollectionName;

    this.events = client.db().collection(eventsCollectionName);
    this.positions = client.db().collection(positionsCollectionName);
    this.snapshots = client.db().collection(snapshotsCollectionName);
    this.transactions = client.db().collection(transactionsCollectionName);
  }

  public setEventToDispatched = (id, callback) => {
    var updateCommand = { '$unset' : { 'dispatched': null } };
    this.events.updateOne({'_id' : id}, updateCommand, callback);
  }

  public removeTransactions = (evt, callback?) => {
    if (!evt.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      if (callback) callback(new Error(errMsg));
      return;
    }

    var findStatement: Filter<TransactionDoc> = { aggregateId: evt.aggregateId };

    if (evt.aggregate) {
      findStatement.aggregate = evt.aggregate;
    }

    if (evt.context) {
      findStatement.context = evt.context;
    }

    // the following is usually unnecessary
    this.transactions.deleteMany(findStatement, function (err) {
      if (err) {
        debug(err);
      }
      if (callback) { callback(err); }
    });
  };

  public repairFailedTransaction = (lastEvt, callback) => {
    this.transactions.findOne({ _id: lastEvt.commitId }, function (error, tx) {
      if (error) {
        debug(error);
        return callback(error);
      }

      if (!tx) {
        var err = new Error('missing tx entry for aggregate ' + lastEvt.aggregateId);
        debug(err);
        return callback(err);
      }

      var missingEvts = tx.events.slice(tx.events.length - lastEvt.restInCommitStream);

      this.events.insertMany(missingEvts, function (err) {
        if (err) {
          debug(err);
          return callback(err);
        }

        this.removeTransactions(lastEvt);

        callback(null);
      });
    });
  };

  public streamEvents = (query, skip, limit) => {
    const findStatement: Filter<EventDoc> = {};

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    if (query.aggregateId) {
      findStatement.aggregateId = query.aggregateId;
    }

    const cursor = this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] });

    if (skip) {
      cursor.skip(skip);
    }
    
    if (limit && limit > 0) {
      cursor.limit(limit);
    }

    return cursor;
  };

  private _streamEventsByRevision = async (findStatement: Filter<EventDoc>, revMin: number, revMax: number, resultStream: any, lastEventArg?: any) => {
    let lastEvent = lastEventArg;
    const filter = findStatement;
    filter.streamRevision = revMax === -1 ? { $gte: revMin } : { $gte: revMin, $lt: revMax };
    const cursor = this.events.find(filter, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] });
    try {
      while (await cursor.hasNext()) {
        const doc = await cursor.next();
        if (!lastEvent) {
          lastEvent = doc;
          return resultStream.write(lastEvent); // Should write the event to resultStream as if there's no lastEvent when there's an event in stream, the event must be first entry of this query.
        }
        
        
        // if for some reason we have written this event alredy
        if ((doc.streamRevision === lastEvent.streamRevision && doc.restInCommitStream <= lastEvent.restInCommitStream) ||
            (doc.streamRevision <= lastEvent.streamRevision)) {
            return;
        }
  
        lastEvent = doc;
        resultStream.write(lastEvent);
      }
    } catch (err) {
      return resultStream.destroy(err);
    }

    if (!lastEvent) {
      return resultStream.end();
    }

    const txOk = (revMax === -1 && !lastEvent.restInCommitStream) ||
                (revMax !== -1 && (lastEvent.streamRevision === revMax - 1 || !lastEvent.restInCommitStream));

    if (txOk) {
      // the following is usually unnecessary
      this.removeTransactions(lastEvent);
      resultStream.end(); // lastEvent was keep duplicated from this line. We should not re-write last event into the stream when ending it. thus end() rather then end(lastEvent).
    }

    this.repairFailedTransaction(lastEvent, function (err) {
      if (err) {
        if (err.message.indexOf('missing tx entry') >= 0) {
          return resultStream.end(lastEvent); // Maybe we should check on this line too?
        }
        debug(err);
        return resultStream.destroy(err);
      }

      this._streamEventsByRevision(findStatement, lastEvent.revMin, revMax, resultStream, lastEvent);
    });
  }

  public streamEventsByRevision = async (query, revMin, revMax) => {
    if (!query.aggregateId) {
      var errMsg = 'aggregateId not defined!';
      debug(errMsg);
      return;
    }

    const findStatement: Filter<EventDoc> = {
      aggregateId: query.aggregateId,
    };

    if (query.aggregate) {
      findStatement.aggregate = query.aggregate;
    }

    if (query.context) {
      findStatement.context = query.context;
    }

    var resultStream = new stream.PassThrough({ objectMode: true, highWaterMark: 1 });
    this._streamEventsByRevision(findStatement, revMin, revMax, resultStream);
    return resultStream;
  }

  public streamEventsSince = (date, skip, limit) => {
    var findStatement: Filter<EventDoc> = { commitStamp: { '$gte': date } };

    var cursor = this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] });

    if (skip)
    cursor.skip(skip);
    
    if (limit && limit > 0)
    cursor.limit(limit);
    
    return cursor;
  };
}

// const streamEventsByRevision = (self, findStatement: Filter<EventDoc>, revMin, revMax, resultStream, lastEvent) => {

//   findStatement.streamRevision = (revMax === -1) ? { '$gte': revMin } : { '$gte': revMin, '$lt': revMax };

//   var mongoStream = self.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] });

//   async.during(function(clb) {
//     mongoStream.hasNext(clb);
//   },
//   function(clb) {
//       mongoStream.next(function(error, e) {
//       if (error)
//         return clb(error);

//       if (!lastEvent) {
//         lastEvent = e;
//         return resultStream.write(lastEvent, clb); // Should write the event to resultStream as if there's no lastEvent when there's an event in stream, the event must be first entry of this query.
//       }

//       // if for some reason we have written this event alredy
//       if ((e.streamRevision === lastEvent.streamRevision && e.restInCommitStream <= lastEvent.restInCommitStream) ||
//           (e.streamRevision <= lastEvent.streamRevision)) {
//           return clb();
//       }

//       lastEvent = e;
//       resultStream.write(lastEvent, clb);
//     });
//   },
//   function (error) {
//     if (error) {
//       return resultStream.destroy(error);
//     }

//     if (!lastEvent) {
//       return resultStream.end();
//     }

//     var txOk = (revMax === -1 && !lastEvent.restInCommitStream) ||
//                 (revMax !== -1 && (lastEvent.streamRevision === revMax - 1 || !lastEvent.restInCommitStream));

//     if (txOk) {
//       // the following is usually unnecessary
//       self.removeTransactions(lastEvent);
//       resultStream.end(); // lastEvent was keep duplicated from this line. We should not re-write last event into the stream when ending it. thus end() rather then end(lastEvent).
//     }

//     self.repairFailedTransaction(lastEvent, function (err) {
//       if (err) {
//         if (err.message.indexOf('missing tx entry') >= 0) {
//           return resultStream.end(lastEvent); // Maybe we should check on this line too?
//         }
//         debug(err);
//         return resultStream.destroy(error);
//       }

//       streamEventsByRevision(self, findStatement, lastEvent.revMin, revMax, resultStream, lastEvent);
//     });
//   });
// };
  
function Mongo(options) {
//   options = options || {};

//   Store.call(this, options);

//   var defaults = {
//     host: 'localhost',
//     port: 27017,
//     dbName: 'eventstore',
//     eventsCollectionName: 'events',
//     snapshotsCollectionName: 'snapshots',
//     transactionsCollectionName: 'transactions'//,
//     // heartbeat: 60 * 1000
//   };

//   _.defaults(options, defaults);

//   var defaultOpt = {
//     autoReconnect: true,
//     ssl: false,
//     useNewUrlParser: true,
//     useUnifiedTopology: true,
//   };

//   options.options = options.options || {};

//   _.defaults(options.options, defaultOpt);

//   this.options = options;
}

util.inherits(MongoClass, Store);

// _.extend(Mongo.prototype, {

  // connect: async (callback) => {
  //   var self = this;

  //   var options = this.options;

  //   var connectionUrl;

  //   if (options.url) {
  //     connectionUrl = options.url;
  //   } else {
  //     var members = options.servers
  //       ? options.servers
  //       : [{host: options.host, port: options.port}];

  //     var memberString = _(members).map(function(m) { return m.host + ':' + m.port; });
  //     var authString = options.username && options.password
  //       ? options.username + ':' + options.password + '@'
  //       : '';
  //     var optionsString = options.authSource
  //       ? '?authSource=' + options.authSource
  //       : '';

  //     connectionUrl = 'mongodb://' + authString + memberString + '/' + options.dbName + optionsString;
  //   }

  //   const ensureIndex = 'createIndex';
  //   const client = new MongoClient(connectionUrl, options.options);
  //   await client.connect();

  //   function initDb() {
  //     client.on('close', function() {
  //       self.emit('disconnect');
  //       self.stopHeartbeat();
  //     });


  //     function finish (err?: any) {
  //       if (err) {
  //         debug(err);
  //         if (callback) callback(err);
  //         return;
  //       }

  //       self.events = self.db.collection(options.eventsCollectionName);
  //       self.events[ensureIndex]({ aggregateId: 1, streamRevision: 1 },
  //         function (err) { if (err) { debug(err); } });
  //       self.events[ensureIndex]({ commitStamp: 1 },
  //         function (err) { if (err) { debug(err); } });
  //       self.events[ensureIndex]({ dispatched: 1 }, { sparse: true },
  //         function (err) { if (err) { debug(err); } });
  //       self.events[ensureIndex]({ commitStamp: 1, streamRevision: 1, commitSequence: 1 },
  //         function (err) { if (err) { debug(err); } });
  
  //       self.snapshots = self.db.collection(options.snapshotsCollectionName);
  //       self.snapshots[ensureIndex]({ aggregateId: 1, revision: -1 },
  //         function (err) { if (err) { debug(err); } });

  //       self.transactions = self.db.collection(options.transactionsCollectionName);
  //       self.transactions[ensureIndex]({ aggregateId: 1, 'events.streamRevision': 1 },
  //         function (err) { if (err) { debug(err); } });
  //       self.events[ensureIndex]({ aggregate: 1, aggregateId: 1, commitStamp: -1, streamRevision: -1, commitSequence: -1 },
  //         function (err) { if (err) { debug(err); } });

  //       if (options.positionsCollectionName) {
  //         self.positions = self.db.collection(options.positionsCollectionName);
  //         self.positionsCounterId = options.eventsCollectionName;
  //       }

  //       self.emit('connect');
  //       if (self.options.heartbeat) {
  //         self.startHeartbeat();
  //       }
  //       if (callback) callback(null, self);
  //     }

  //     finish();
  //   }
  // },

  // stopHeartbeat: function () {
  //   if (this.heartbeatInterval) {
  //     clearInterval(this.heartbeatInterval);
  //     delete this.heartbeatInterval;
  //   }
  // },

  // startHeartbeat: function () {
  //   var self = this;

  //   var gracePeriod = Math.round(this.options.heartbeat / 2);
  //   this.heartbeatInterval = setInterval(function () {
  //     var graceTimer = setTimeout(function () {
  //       if (self.heartbeatInterval) {
  //         console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (mongodb)')).stack);
  //         self.disconnect();
  //       }
  //     }, gracePeriod);

  //     self.db.command({ ping: 1 }, function (err) {
  //       if (graceTimer) clearTimeout(graceTimer);
  //       if (err) {
  //         console.error(err.stack || err);
  //         self.disconnect();
  //       }
  //     });
  //   }, this.options.heartbeat);
  // },

  // disconnect: function (callback) {
  //   this.stopHeartbeat();

  //   if (!this.db) {
  //     if (callback) callback(null);
  //     return;
  //   }

  //   this.db.close(function (err) {
  //     if (err) {
  //       debug(err);
  //     }
  //     if (callback) callback(err);
  //   });
  // },

  // clear: function (callback) {
  //   var self = this;
  //   async.parallel([
  //     function (callback) {
  //       self.events.deleteMany({}, callback);
  //     },
  //     function (callback) {
  //       self.snapshots.deleteMany({}, callback);
  //     },
  //     function (callback) {
  //       self.transactions.deleteMany({}, callback);
  //     },
  //     function (callback) {
  //       if (!self.positions)
  //         return callback(null);
  //       self.positions.deleteMany({}, callback);
  //     }
  //   ], function (err) {
  //     if (err) {
  //       debug(err);
  //     }
  //     if (callback) callback(err);
  //   });
  // },

  // getNewId: function(callback) {
  //   callback(null, new ObjectID().toString());
  // },

  // getNextPositions: function(positions, callback) {
  //   if (!this.positions)
  //     return callback(null);

  //   this.positions.findOneAndUpdate({ _id: this.positionsCounterId }, { $inc: { position: positions } }, { returnOriginal: false, upsert: true }, function (err, pos) {
  //     if (err)
  //       return callback(err);

  //     pos.value.position += 1;

  //     callback(null, _.range(pos.value.position - positions, pos.value.position));
  //   });
  // },

  // addEvents: function (events, callback) {
  //   if (events.length === 0) {
  //     if (callback) { callback(null); }
  //     return;
  //   }

  //   var commitId = events[0].commitId;

  //   var noAggregateId = false,
  //     invalidCommitId = false;

  //   var self = this;
  //   // this.getNextPositions(events.length, function(err, positions) {
  //     /*
  //     if (err) {
  //       debug(err);
  //       if (callback) callback(err);
  //       return;
  //     }
  //     */

  //     _.forEach(events, function (evt, index) {
  //       if (!evt.aggregateId) {
  //         noAggregateId = true;
  //       }

  //       if (!evt.commitId || evt.commitId !== commitId) {
  //         invalidCommitId = true;
  //       }

  //       evt._id = evt.id;
  //       evt.dispatched = false;
  //     });

  //     if (noAggregateId) {
  //       var errMsg = 'aggregateId not defined!';
  //       debug(errMsg);
  //       if (callback) callback(new Error(errMsg));
  //       return;
  //     }

  //     if (invalidCommitId) {
  //       var errMsg = 'commitId not defined or different!';
  //       debug(errMsg);
  //       if (callback) callback(new Error(errMsg));
  //       return;
  //     }

  //     if (events.length === 1) {
  //       return self.events.insertOne(events[0], callback);
  //     }

  //     var tx = {
  //       _id: commitId,
  //       events: events,
  //       aggregateId: events[0].aggregateId,
  //       aggregate: events[0].aggregate,
  //       context: events[0].context
  //     };

  //     self.transactions.insertOne(tx, function (err) {
  //       if (err) {
  //         debug(err);
  //         if (callback) callback(err);
  //         return;
  //       }

  //       self.events.insertMany(events, function (err) {
  //         if (err) {
  //           debug(err);
  //           if (callback) callback(err);
  //           return;
  //         }

  //         self.removeTransactions(events[events.length - 1], callback);
  //       });
  //     });
  //   // });
  // },

  // streaming API
  // streamEvents: function (query, skip, limit) {
  //   const findStatement: Filter<EventDoc> = {};

  //   if (query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query.context) {
  //     findStatement.context = query.context;
  //   }

  //   if (query.aggregateId) {
  //     findStatement.aggregateId = query.aggregateId;
  //   }

  //   var query = this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] });

  //   if (skip) {
  //     query.skip(skip);
  //   }
    
  //   if (limit && limit > 0) {
  //     query.limit(limit);
  //   }

  //   return query;
  // },

  // streamEventsSince: function (date, skip, limit) {
  //   var findStatement = { commitStamp: { '$gte': date } };

  //   var query = this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] });

  //   if (skip)
  //     query.skip(skip);
    
  //   if (limit && limit > 0)
  //     query.limit(limit);
    
  //   return query;
  // },

  // streamEventsByRevision: function (query, revMin, revMax) {
  //   if (!query.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   const findStatement: Filter<EventDoc> = {
  //     aggregateId: query.aggregateId,
  //   };

  //   if (query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query.context) {
  //     findStatement.context = query.context;
  //   }

  //   var self = this;

  //   var resultStream = new stream.PassThrough({ objectMode: true, highWaterMark: 1 });
  //   streamEventsByRevision(self, findStatement, revMin, revMax, resultStream);
  //   return resultStream;
  // },

  // getEvents: function (query, skip, limit, callback) {
  //   this.streamEvents(query, skip, limit).toArray(callback);
  // },

  // getEventsSince: function (date, skip, limit, callback) {
  //   this.streamEventsSince(date, skip, limit).toArray(callback);
  // },

  // getEventsByRevision: function (query, revMin, revMax, callback) {
  //   if (!query.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   var streamRevOptions = { '$gte': revMin, '$lt': revMax };
  //   if (revMax === -1) {
  //     streamRevOptions = { '$gte': revMin };
  //   }

  //   var findStatement = {
  //     aggregateId: query.aggregateId,
  //     streamRevision: streamRevOptions
  //   };

  //   if (query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query.context) {
  //     findStatement.context = query.context;
  //   }

  //   var self = this;

  //   this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] }).toArray(function (err, res) {
  //     if (err) {
  //       debug(err);
  //       return callback(err);
  //     }

  //     if (!res || res.length === 0) {
  //       return callback(null, []);
  //     }

  //     var lastEvt = res[res.length - 1];

  //     var txOk = (revMax === -1 && !lastEvt.restInCommitStream) ||
  //                (revMax !== -1 && (lastEvt.streamRevision === revMax - 1 || !lastEvt.restInCommitStream));

  //     if (txOk) {
  //       // the following is usually unnecessary
  //       self.removeTransactions(lastEvt);

  //       return callback(null, res);
  //     }

  //     self.repairFailedTransaction(lastEvt, function (err) {
  //       if (err) {
  //         if (err.message.indexOf('missing tx entry') >= 0) {
  //           return callback(null, res);
  //         }
  //         debug(err);
  //         return callback(err);
  //       }

  //       self.getEventsByRevision(query, revMin, revMax, callback);
  //     });
  //   });
  // },

  // getUndispatchedEvents: function (query, callback) {
  //   var findStatement = {
  //     dispatched: false
  //   };

  //   if (query && query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query && query.context) {
  //     findStatement.context = query.context;
  //   }

  //   if (query && query.aggregateId) {
  //     findStatement.aggregateId = query.aggregateId;
  //   }

  //   this.events.find(findStatement, { sort: [['commitStamp', 'asc'], ['streamRevision', 'asc'], ['commitSequence', 'asc']] }).toArray(callback);
  // },

  // setEventToDispatched: function (id, callback) {
  //   var updateCommand = { '$unset' : { 'dispatched': null } };
  //   this.events.updateOne({'_id' : id}, updateCommand, callback);
  // },

  // addSnapshot: function(snap, callback) {
  //   if (!snap.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   snap._id = snap.id;
  //   this.snapshots.insertOne(snap, callback);
  // },

  // cleanSnapshots: function (query, callback) {
  //   if (!query.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   var findStatement: Filter<SnapshotDoc> = {
  //     aggregateId: query.aggregateId
  //   };

  //   if (query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query.context) {
  //     findStatement.context = query.context;
  //   }

  //   this.snapshots.find(findStatement, {
  //     sort: [['revision', 'desc'], ['version', 'desc'], ['commitStamp', 'desc']]
  //   })
  //     .skip(this.options.maxSnapshotsCount)
  //     .toArray(removeElements(this.snapshots, callback));
  // },

  // getSnapshot: function (query, revMax, callback) {
  //   if (!query.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   var findStatement = {
  //     aggregateId: query.aggregateId
  //   };

  //   if (query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query.context) {
  //     findStatement.context = query.context;
  //   }

  //   if (revMax > -1) {
  //     findStatement.revision = { '$lte': revMax };
  //   }

  //   this.snapshots.findOne(findStatement, { sort: [['revision', 'desc'], ['version', 'desc'], ['commitStamp', 'desc']] }, callback);
  // },

  // removeTransactions: function (evt, callback) {
  //   if (!evt.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   var findStatement = { aggregateId: evt.aggregateId };

  //   if (evt.aggregate) {
  //     findStatement.aggregate = evt.aggregate;
  //   }

  //   if (evt.context) {
  //     findStatement.context = evt.context;
  //   }

  //   // the following is usually unnecessary
  //   this.transactions.deleteMany(findStatement, function (err) {
  //     if (err) {
  //       debug(err);
  //     }
  //     if (callback) { callback(err); }
  //   });
  // },

  // getPendingTransactions: function (callback) {
  //   var self = this;
  //   this.transactions.find({}).toArray(function (err, txs) {
  //     if (err) {
  //       debug(err);
  //       return callback(err);
  //     }

  //     if (txs.length === 0) {
  //       return callback(null, txs);
  //     }

  //     var goodTxs = [];

  //     async.map(txs, function (tx, clb) {
  //       var findStatement = { commitId: tx._id, aggregateId: tx.aggregateId };

  //       if (tx.aggregate) {
  //         findStatement.aggregate = tx.aggregate;
  //       }

  //       if (tx.context) {
  //         findStatement.context = tx.context;
  //       }

  //       self.events.findOne(findStatement, function (err, evt) {
  //         if (err) {
  //           return clb(err);
  //         }

  //         if (evt) {
  //           goodTxs.push(evt);
  //           return clb(null);
  //         }
          
  //         self.transactions.deleteOne({ _id: tx._id }, clb);
  //       });
  //     }, function (err) {
  //       if (err) {
  //         debug(err);
  //         return callback(err);
  //       }

  //       callback(null, goodTxs);
  //     })
  //   });
  // },

  // getLastEvent: function (query, callback) {
  //   if (!query.aggregateId) {
  //     var errMsg = 'aggregateId not defined!';
  //     debug(errMsg);
  //     if (callback) callback(new Error(errMsg));
  //     return;
  //   }

  //   const findStatement: Filter<EventDoc> = { aggregateId: query.aggregateId };

  //   if (query.aggregate) {
  //     findStatement.aggregate = query.aggregate;
  //   }

  //   if (query.context) {
  //     findStatement.context = query.context;
  //   }

  //   this.events.findOne(findStatement, { sort: [['commitStamp', 'desc'], ['streamRevision', 'desc'], ['commitSequence', 'desc']] }, callback);
  // },

  // repairFailedTransaction: function (lastEvt, callback) {
  //   var self = this;

  //   //var findStatement = {
  //   //  aggregateId: lastEvt.aggregateId,
  //   //  'events.streamRevision': lastEvt.streamRevision + 1
  //   //};
  //   //
  //   //if (lastEvt.aggregate) {
  //   //  findStatement.aggregate = lastEvt.aggregate;
  //   //}
  //   //
  //   //if (lastEvt.context) {
  //   //  findStatement.context = lastEvt.context;
  //   //}

  //   //this.transactions.findOne(findStatement, function (err, tx) {
  //   this.transactions.findOne({ _id: lastEvt.commitId }, function (err, tx) {
  //     if (err) {
  //       debug(err);
  //       return callback(err);
  //     }

  //     if (!tx) {
  //       var err = new Error('missing tx entry for aggregate ' + lastEvt.aggregateId);
  //       debug(err);
  //       return callback(err);
  //     }

  //     var missingEvts = tx.events.slice(tx.events.length - lastEvt.restInCommitStream);

  //     self.events.insertMany(missingEvts, function (err) {
  //       if (err) {
  //         debug(err);
  //         return callback(err);
  //       }

  //       self.removeTransactions(lastEvt);

  //       callback(null);
  //     });
  //   });
  // }

// });

const removeElements = (
  collection: Collection<Record<'_id', string>>,
  callback: (error: Error, elements?: Array<Record<'_id', string>>) => void,
) => {
  return function (error, elements) {
    if (error) {
      debug(error);
      return callback(error);
    }
    async.each(elements, function (element, callback) {
      try {
        collection.deleteOne({ _id: element._id });
        callback();
      } catch (error) {
        callback(error);
      }
    }, function(error) {
      callback(error, elements.length);
    });
  }
}

module.exports = MongoClass;
