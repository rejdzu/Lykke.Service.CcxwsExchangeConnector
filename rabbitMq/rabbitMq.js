const amqp = require('amqplib')
const path = require('path');
const LogFactory = require('../utils/logFactory')

class RabbitMq {

    constructor(settings, loggingLevel) {
        this._settings = settings;
        this._log = LogFactory.create(path.basename(__filename), loggingLevel)
      }

    async getChannel() {
        if (!this._channel)
            this._channel = await this._createChannel()

        return this._channel
    }

    async _createChannel() {
        try
        {
            const connection = await amqp.connect(this._settings.ConnectionString)
            const channel = await connection.createChannel()
            await channel.assertExchange(this._settings.OrderBooks, 'fanout', {durable: false})
            await channel.assertExchange(this._settings.TickPrices, 'fanout', {durable: false})
        
            return channel
        }
        catch (e)
        {
            this._log.warn(`Exception while trying to create RabbitMq channel: ${e}, ${e.stack}.`)
        }
    }

    async send(rabbitExchange, object) {
        const objectJson = JSON.stringify(object)
    
        try {
            const channel = await this.getChannel()
            channel.publish(rabbitExchange, '', new Buffer(objectJson))
        }
        catch(e) {
            this._log.warn(`Error while sending a message to rabbit: ${e}, ${e.stack}.`)
        }
    }

}

module.exports = RabbitMq