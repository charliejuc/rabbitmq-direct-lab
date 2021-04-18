'use strict'

const amqp = require('amqplib')
const {backOff} = require('../lib/backoff')
const queue = process.env.QUEUE || 'hello'
const exchangeName = process.env.EXCHANGE || 'my-direct'
const difficulty = process.env.DIFFICULTY != null
    ? Number(process.env.DIFFICULTY)
    : 9
const exchangeType = 'direct'
const pattern = process.env.PATTERN || ''

console.log({
    queue,
    exchangeName,
    pattern
})

function intensiveOperation() {
    const maxDifficulty = 10 ** difficulty
    const minDifficulty = Math.floor(maxDifficulty * .8)

    let i = minDifficulty + Math.floor(Math.random() * (maxDifficulty - minDifficulty))
    while (i--) {
    }
}

async function subscriber() {
    const backOffMinTime1MaxTime4 = backOff(1)(4)

    const main = async () => {
        const connection = await amqp.connect('amqp://localhost')
        const channel = await connection.createChannel()

        channel.prefetch(1)

        channel.on('close', () => {
            mainBackOff()
        })

        await channel.assertQueue(queue, {
            durable: true
        })

        await channel.assertExchange(exchangeName, exchangeType, {
            // durable: true
        })

        await channel.bindQueue(queue, exchangeName, pattern)

        channel.consume(queue, (message) => {
            const content = JSON.parse(message.content.toString())

            console.log(`Received message from "${queue}" queue`)
            console.log(content)

            intensiveOperation()

            console.log('DONE!')

            channel.ack(message)
        })
    }

    const onErrorEnd = (error) => {
        console.error(error)
        mainBackOff().catch(console.error)
    }
    const mainBackOff = backOffMinTime1MaxTime4(
        main,
        onErrorEnd,
        console.log,
        console.error
    )

    mainBackOff()
}

subscriber().catch((error) => {
    console.error(error)
    process.exit(1)
})
