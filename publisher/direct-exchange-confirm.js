'use strict'

const amqp = require('amqplib')
const exchangeName = process.env.EXCHANGE || 'my-direct'
const routingKey = process.env.ROUTING_KEY || ''
const delay = process.env.DELAY != null
    ? Number(process.env.DELAY)
    : 3000
const exchangeType = 'direct'
const maxErrors = 5

console.log({
    exchangeName,
    exchangeType,
    routingKey
})

async function publisher() {
    const sendMessage = async (connection, channel, errorsCount) => {
        try {
            if (errorsCount >= maxErrors || connection === null) {
                if (connection !== null) {
                    connection.close()
                }

                const _connection = await amqp.connect('amqp://localhost')
                const _channel = await _connection.createConfirmChannel()

                _channel.assertExchange(exchangeName, exchangeType, {
                    // durable: true
                })

                await sendMessage(_connection, _channel, 0)
                return
            }

            const message = {
                id: Math.random().toString(32).slice(2, 6),
                text: 'Hello world!'
            }

            channel.publish(
                exchangeName,
                routingKey,
                Buffer.from(JSON.stringify(message)),
                {
                    // persistent: true,
                }
            )

            await channel.waitForConfirms()
            console.log(`Message sent to "${exchangeName}" exchange confirmed`, message)
            setTimeout(sendMessage, delay, connection, channel, 0)
        } catch (error) {
            console.error(error, {errorsCount})
            setTimeout(sendMessage, delay, connection, channel, errorsCount + 1)
        }
    }

    setTimeout(sendMessage, delay, null, null, 0)
}

publisher().catch((error) => {
    console.error(error)
    process.exit(1)
})
