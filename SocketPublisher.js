// Copyright (c) Lykke Corp.
// See the LICENSE file in the project root for more information.

const path = require('path');
const LogFactory = require('./utils/logFactory');

class SocketPublisher {

    constructor(socket, port, host, settings) {
        this._socket = socket;
        this._log = LogFactory.create(path.basename(__filename), settings.Main.LoggingLevel);

        this._socket.connect(port, host, () => this._log.info('Sanitizer connected on: ' + port + ':' + host));
    }

    async publishBidAsk(tick) {
        return new Promise((resolve, reject) => {
            this._socket.write(this._createFrame("quote", tick), (err) => {
                err ? reject(err) : resolve
            });
        });
    }

    async publishTrade(trade) {
        return new Promise((resolve, reject) => {
            this._socket.write(this._createFrame("trade", trade), (err) => {
                err ? reject(err) : resolve
            });
        });
    }

    _createFrame(type, data) {
        const obj = {
            type: type,
            data: data
        }

        return `${JSON.stringify(obj)}\n`
    }
}

module.exports = SocketPublisher;
