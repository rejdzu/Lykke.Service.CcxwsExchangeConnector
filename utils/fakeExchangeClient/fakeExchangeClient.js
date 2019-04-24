const { EventEmitter } = require("events");
const winston = require("winston");
const { wait } = require("./util");
const Ticker = require("./ticker");
const Trade = require("./trade");
const Level2Point = require("./level2-point");
const Level2Update = require("./level2-update");
const Level2Snapshot = require("./level2-snapshot");

class FakeExchangeClient extends EventEmitter {
  constructor(name) {
    super();
    this._name = name;
    this._baseValue = this._getBaseValue()
    this._tickerSubs = new Map();
    this._tradeSubs = new Map();
    this._level2SnapshotSubs = new Map();
    this._level2UpdateSubs = new Map();
    this._reconnectDebounce = undefined;

    this.hasTickers = true;
    this.hasTrades = true;
    this.hasLevel2Snapshots = true;
    this.hasLevel2Updates = true;
    this.hasLevel3Snapshots = false;
    this.hasLevel3Updates = false;
  }

  //////////////////////////////////////////////

  subscribeTicker(market) {
    this._subscribe(market, "subscribing to ticker", this._tickerSubs);
  }

  unsubscribeTicker(market) {
    this._unsubscribe(market, "unsubscribing from ticker", this._tickerSubs);
  }

  subscribeTrades(market) {
    this._subscribe(market, "subscribing to trades", this._tradeSubs);
  }

  unsubscribeTrades(market) {
    this._unsubscribe(market, "unsubscribing to trades", this._tradeSubs);
  }

  subscribeLevel2Snapshots(market) {
    this._subscribe(market, "subscribing to l2 snapshots", this._level2SnapshotSubs);
  }

  unsubscribeLevel2Snapshots(market) {
    this._unsubscribe(market, "unsubscribing from l2 snapshots", this._level2SnapshotSubs);
  }

  subscribeLevel2Updates(market) {
    this._subscribe(market, "subscribing to l2 upates", this._level2UpdateSubs);
  }

  unsubscribeLevel2Updates(market) {
    this._unsubscribe(market, "unsubscribing from l2 updates", this._level2UpdateSubs);
  }

  reconnect() {
    winston.info("reconnecting");
    this._reconnect();
    this.emit("reconnected");
  }

  close() {
    this._close();
  }

  ////////////////////////////////////////////
  // PROTECTED

  _getBaseValue() {
    return Math.random() * 100
  }

  _subscribe(market, msg, map) {
    let remote_id = market.id.toLowerCase();
    if (!map.has(remote_id)) {
      winston.info(msg, this._name, remote_id);
      map.set(remote_id, market);
      this._reconnect();
    }
  }

  _unsubscribe(market, msg, map) {
    let remote_id = market.id.toLowerCase();
    if (map.has(remote_id)) {
      winston.info(msg, this._name, remote_id);
      map.delete(remote_id);
      this._reconnect();
    }
  }

  /**
   * Reconnects the socket after a debounce period
   * so that multiple calls don't cause connect/reconnect churn
   */
  _reconnect() {
    clearTimeout(this._reconnectDebounce);
    this._reconnectDebounce = setTimeout(() => {
      this._close();
      this._connect();
    }, 100);
  }

  /**
   * Close the underlying connction, which provides a way to reset the things
   */
  _close() {
    this.emit("closed");
  }

  /** Connect to the websocket stream by constructing a path from
   * the subscribed markets.
   */
  _connect() {
    let nextTick = function() {
      this._generateRandomEvent();
      setTimeout(nextTick,  Math.random() * 200);
    }
    nextTick = nextTick.bind(this)
    nextTick();
  }

  ////////////////////////////////////////////
  // ABSTRACT

  _onConnected() {
    this._watcher.start();
    this._requestLevel2Snapshots(); // now that we're connected...
    this.emit("connected");
  }

  _onDisconnected() {
    this._watcher.stop();
    this.emit("disconnected");
  }

  _generateRandomEvent() {
    const v = Math.random()
    if(v > 0.7) {
      let trade = this._constructRawTrade();
      this.emit("trade", trade);
    } else {
      if(this._level2UpdateSubs.size > 0) {
        let update = this._constructLevel2Update();
        this.emit("l2update", update);  
      } else {
        let snapshot = this._constructLevel2Snapshot();
        this.emit("l2snapshot", snapshot);
      }
    }
  }

  _constructTicker(msg) {
  }

  _getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  _generateAmount() {
    return Math.random() * 10;
  }

  _generatePrice() {
    const sign = Math.random() < 0.5 ? 1 : -1;
    const delta = sign * Math.random() * 10
    return this._baseValue + delta
  }

  _generateLevel2PointArray(count) {
    const array = []
    for(let i = 0; i < count; i++) {
      const point = new Level2Point(this._generatePrice(), this._generateAmount())
      array.push(point)
    }
    return array
  }

  _constructRawTrade() {
    const index = this._getRandomInt(0, this._tradeSubs.size - 1)
    const symbol = Array.from(this._tradeSubs)[index]
    const market = this._tradeSubs.get(symbol[0].toLowerCase());
    const unix = new Date();
    const amount = this._generateAmount()
    const price = this._generatePrice()
    const side = Math.random() > 0.5 ? "buy" : "sell";
    return new Trade({
      exchange: this._name,
      base: market.base,
      quote: market.quote,
      tradeId: this._name + Math.floor(Math.random() * 1000),
      unix,
      side,
      price,
      amount,
      buyOrderId: "",
      sellOrderId: "",
    });
  }

  _constructLevel2(count, map) {
    const index = this._getRandomInt(0, map.size - 1)
    const symbol = Array.from(map)[index]
    const market = map.get(symbol[0].toLowerCase());
    const sequenceId = "";
    const asks = this._generateLevel2PointArray(count)
    const bids = this._generateLevel2PointArray(count)
    return new Level2Snapshot({
      exchange: this._name,
      base: market.base,
      quote: market.quote,
      sequenceId,
      asks,
      bids,
    });
  }

  _constructLevel2ForMarket(count, marketId) {
    const market = this._level2SnapshotSubs.get(marketId);
    const sequenceId = "";
    const asks = this._generateLevel2PointArray(count)
    const bids = this._generateLevel2PointArray(count)
    return new Level2Snapshot({
      exchange: this._name,
      base: market.base,
      quote: market.quote,
      sequenceId,
      asks,
      bids,
    });
  }

  _constructLevel2Snapshot() {
    return this._constructLevel2(100, this._level2SnapshotSubs)
  }

  _constructLevel2Update(msg) {
    return this._constructLevel2(_getRandomInt(0, 20), this._level2UpdateSubs)
  }

  _requestLevel2Snapshots() {
    for (let market of this._level2SnapshotSubs.values()) {
      this._constructLevel2Snapshot(100, market.id)
    }
  }
}

module.exports = FakeExchangeClient;