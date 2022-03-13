import { resolve } from 'path';

import debugLib from 'debug';
import _ from 'lodash';

import Base from './lib/base';
import Eventstore from './lib/eventstore';
import StoreEventEmitter from './lib/storeEventEmitter';

const debug = debugLib('eventstore');

const exists = (toCheck: string): boolean => {
  let _exists = require('fs').existsSync || require('path').existsSync;
  if (require('fs').accessSync) {
    _exists = function (toCheck) {
      try {
        require('fs').accessSync(toCheck);
        return true;
      } catch (e) {
        return false;
      }
    };
  }
  return _exists(toCheck);
}

const getSpecificStore = (options) => {
  options = options || {};

  options.type = options.type || 'inmemory';

  if (_.isFunction(options.type)) {
    return options.type;
  }

  options.type = options.type.toLowerCase();

  const dbPath = resolve(__dirname, 'lib', 'databases', `${options.type}.js`);

  if (!exists(dbPath)) {
    const errMsg = `Implementation for db ${options.type} does not exist!`;
    console.log(errMsg);
    debug(errMsg);
    throw new Error(errMsg);
  }

  try {
    const db = require(dbPath);
    return db;
  } catch (err) {

    if (err.message.indexOf('Cannot find module') >= 0 &&
      err.message.indexOf("'") > 0 &&
      err.message.lastIndexOf("'") !== err.message.indexOf("'")) {

      const moduleName = err.message.substring(err.message.indexOf("'") + 1, err.message.lastIndexOf("'"));
      const msg = `Please install module ${moduleName} to work with db implementation ${options.type}.`;
      console.log(msg);
      debug(msg);
    }

    throw err;
  }
}

const getStore = (options) => {
  options = options || {};

  let Store;

  try {
    Store = getSpecificStore(options);
  } catch (err) {
    throw err;
  }

  const eventstore = new Eventstore(options, new Store(options));

  if (options.emitStoreEvents) {
    const storeEventEmitter = new StoreEventEmitter(eventstore);
    storeEventEmitter.addEventEmitter();
  }

  return eventstore;
};

export default getStore;
export { Base as Store };
