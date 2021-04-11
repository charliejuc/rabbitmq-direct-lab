'use strict'

const amqp = require('amqplib')
const exchangeName = process.env.EXCHANGE || 'my-direct'
const routingKey = process.env.ROUTING_KEY || ''
const delay = process.env.DELAY != null
    ? Number(process.env.DELAY)
    : 3000
const exchangeType = 'direct'

console.log({
    exchangeName,
    exchangeType,
    routingKey
})

async function publisher() {
    const connection = await amqp.connect('amqp://localhost')
    const channel = connection.createChannel()

    channel.assertExchange(exchangeName, exchangeType, /*{
        durable: true
    }*/)

    setInterval(() => {
        const message = {
            id: Math.random().toString(32).slice(2, 6),
            text: 'Hello world!'
        }

        const sent = channel.publish(
            exchangeName,
            routingKey,
            Buffer.from(JSON.stringify(message)),
            {
                // persistent: true
            }
        )

        sent
            ? console.log(`Sent message to "${exchangeName}" exchange`, message)
            : console.log(
            `Fails sending message to "${exchangeName}" exchange`,
            message
            )
    }, delay)
}

publisher().catch((error) => {
    console.error(error)
    process.exit(1)
})
