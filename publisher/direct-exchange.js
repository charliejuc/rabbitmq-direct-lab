'use strict'

const amqp = require('amqplib')
const {backOff} = require('../lib/backoff')

const exchangeName = process.env.EXCHANGE || 'my-direct'
const routingKey = process.env.ROUTING_KEY || ''
const delay = process.env.DELAY != null
    ? Number(process.env.DELAY)
    : 2000
const exchangeType = 'direct'

console.log({
    exchangeName,
    exchangeType,
    routingKey
})

async function publisher() {
    const messages = []

    const sendMessage = async (connection, channel, message) => {
        const sent = channel.publish(
            exchangeName,
            routingKey,
            Buffer.from(JSON.stringify(message)),
            {
                // persistent: true
            }
        )

        if (sent) {
            console.log(`Sent message to "${exchangeName}" exchange`, message)
            return
        }

        console.error({sent})
        throw new Error(`Fail sending message to "${exchangeName}" exchange, "${JSON.stringify(message)}"`)
    }

    const backOffMinTime1MaxTime4 = backOff(1)(4)
    const backOffMinTime1MaxTime32 = backOff(1)(32)
    const main = async (messages) => {
        const connection = await amqp.connect('amqp://localhost')
        const channel = await connection.createChannel()

        channel.assertExchange(exchangeName, exchangeType, {
            // durable: true
        })

        const sendMessageTimeout = () => {
            if (messages.length > 0) {
                setTimeout(sendMessageBackOff, delay, connection, channel, messages.shift())
                return
            }

            const message = {
                id: Math.random().toString(32).slice(2, 6),
                text: 'Hello world!'
            }

            setTimeout(sendMessageBackOff, delay, connection, channel, message)
        }

        const onErrorEnd = (_, ...args) => {
            connection.close()

            const message = args[2]
            messages.push(message)

            mainBackOff(messages)
        }

        const sendMessageBackOff = backOffMinTime1MaxTime4(
            sendMessage,
            onErrorEnd,
            sendMessageTimeout,
            console.error
        )

        sendMessageTimeout()
    }

    const onErrorEnd = (error) => {
        console.error(error)
        mainBackOff(messages)
    }
    const mainBackOff = backOffMinTime1MaxTime32(
        main,
        onErrorEnd,
        console.log,
        console.error
    )

    mainBackOff(messages)
}

publisher().catch((error) => {
    console.error(error)
    process.exit(1)
})
