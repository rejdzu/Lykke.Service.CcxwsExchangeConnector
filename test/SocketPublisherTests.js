// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const assert = require('chai').assert;
const describe = require('mocha').describe;
const sinon = require('sinon');
const SocketPublisher = require('../SocketPublisher');
const net = require('net');

const settings = {
    Main: { LoggingLevel: 'INFO', }
};

describe('SocketPublisher', () => {
    const tick = {
        source: "Bitstamp(w)",
        asset: "BTCUSD",
        bid: "3900",
        ask: "4300",
        timestamp: '2019-04-11T12:03:24+00:00'
    };
    const fakeSocket = {
        connect: sinon.fake(),
        write: sinon.spy()
    };

    const socketPublisher = new SocketPublisher(fakeSocket, 0, '', settings);

    // Reset fakes before each test
    beforeEach(async () => {
        fakeSocket.connect.resetHistory();
        fakeSocket.write.resetHistory();
    });

    describe('publishBidAsk', () => {
        it("writes a stringified version of the given message to the socket", () => {
            socketPublisher.publishBidAsk(tick);

            assert(fakeSocket.write.calledOnce, "The socket should be written to exactly once");
            assert.equal(fakeSocket.write.args[0][0], JSON.stringify(tick),
                         "The message should be the stringified input");
        });
    });

    describe('publishTrade', () => {
        const trade = {
            exchange: 'bitstamp',
            marketId: 'BTC/USD',
            side: 'buy',
            price: '4300',
            amount: 1
        };

        it("writes a stringified version of the given message to the socket", () => {
            socketPublisher.publishTrade(trade);

            assert(fakeSocket.write.calledOnce, "The socket should be written to exactly once");
            assert.equal(fakeSocket.write.args[0][0], JSON.stringify(trade),
                "The message should be the stringified input");
        });
    });
});
