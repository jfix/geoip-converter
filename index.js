const fs = require('fs')
const parse = require('csv-parse')
const transform = require('stream-transform')
const stringify = require('csv-stringify')
const multistream = require('multistream')
const ipcidr = require('ip-cidr')
const R = require('ramda')


const inputs = [
    fs.createReadStream('data/GeoLite2-Country-Blocks-IPv4.csv'),
    fs.createReadStream('data/GeoLite2-Country-Blocks-IPv6.csv')
]
const geoLookup = fs.createReadStream('data/GeoLite2-Country-Locations-en.csv')
const output = fs.createWriteStream('data/out.csv')
const parser = parse({})
const stringifier = stringify({quoted: true})

const removeHeaders = transform((line, callback) => {
    if (line[0] !== 'network') {
        callback(null, line)
    }
})

// take a CIDR range and return an array [startString, endString, startInt, endInt]
const convertCidrToRange = (cidr) => {
    const network = new ipcidr(cidr)
    const range = network.toRange()
    const start = network.start({type: 'bigInteger'}).toString()
    const end = network.end({type: 'bigInteger'}).toString()
    return R.insertAll(2, [start, end], range)
}

// transforms a incoming line by adding IP addresses and removes the CIDR range
const resolveIps = transform((line, callback) => {
    console.time('resolveIps')
    callback(null, R.pipe(
        R.insertAll(1, convertCidrToRange(line[0])),
    )(line))
}, {
    parallel: 100
}).on('error', (err) => console.log(err.message)
).on('end', () => console.timeEnd('resolveIps'))

const getGeoInfo = (geoid) => {
    const info = R.filter(R.propEq('id', geoid))(geoNames)
    if (R.equals(info.length, 1)) {
        const o = info[0]
        return [o.iso, o.name]
    }
    return ['ZZ', `Unknown ${geoid}`]
}

const resolveGeoNames = transform((line, callback) => {
    const arr = getGeoInfo(line[5])
    const newLine = R.insertAll(5, arr)(line)
    callback(null, newLine)
}, {
    parallel: 100
}).on('error', (err) => console.log(err.message))

const cleanUpCells = transform((line, callback) => {
    const newLine = R.pipe(
        R.remove(0, 1),
        R.remove(6, 5)
    )(line)
    callback(null, newLine)
}, {
    parallel: 100
}).on('error', (err) => console.log(err.message))

const loadGeoNames = new Promise((resolve, reject) => {
    console.time('loadGeoNames')
    let names = []
    const parser = parse({columns:true})
    geoLookup.pipe(parser)
    parser.on('readable', () => {
        let record
        while (record = parser.read()) {
            names.push({
                id: record.geoname_id, 
                name: record.country_name, 
                iso: record. country_iso_code
            })
        }
    })
    parser.on('error', (error) => reject(error))
    parser.on('end', () => {
        console.log(`INFO: loaded ${names.length} geonames.`)
        console.timeEnd('loadGeoNames')
        return resolve(names)
    })
})

let geoNames = [];

(async () => {
    geoNames = await loadGeoNames

    await multistream(inputs)
        .pipe(parser)
        .pipe(removeHeaders)
        .pipe(resolveIps)
        .pipe(resolveGeoNames)
        .pipe(cleanUpCells)
        .pipe(stringifier)
        .pipe(output)
})()
