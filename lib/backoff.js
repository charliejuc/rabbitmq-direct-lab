const increaseBackOffTime = (currentBackoffTime) => currentBackoffTime * 2
const calculateBackOffDelayMs = (backoffTime) => 1000 * (backoffTime + Math.random())

const backOff = (minTime) => (maxTime) => (fn, onErrorEnd, onSuccess, onError) => {
    const _run = (currentTime) => (...args) => {
        setTimeout(async () => {
            try {
                const result = await fn(...args)

                if (onSuccess) {
                    onSuccess(result)
                }
            } catch (error) {
                if (currentTime >= maxTime) {
                    if (onErrorEnd) {
                        onErrorEnd(error, ...args)
                    }
                    return
                }

                if (onError) {
                    onError(error)
                }

                _run(increaseBackOffTime(currentTime))(...args)
            }
        }, calculateBackOffDelayMs(currentTime))
    }

    return _run(minTime)
}

// const p = () => new Promise((resolve, reject) => setTimeout(() => {
//     if (Math.random() > .4) {
//         reject(new Error('Fail!!'))
//         return
//     }
//
//     resolve('Work!')
// }, 300))
//
// const pBackoff = backOff(1)(4)(
//     p, (error) => console.log('END with Errors!', error), console.log, console.error
// )
//
// pBackoff()

module.exports = {
    backOff
}
